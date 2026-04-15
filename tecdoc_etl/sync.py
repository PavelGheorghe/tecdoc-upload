import logging
import os
import re
import shutil
import threading
from pathlib import Path
from typing import Any, Callable

import psycopg2.extensions

from tecdoc_etl.archive_skip import skip_logistics_supplier_id
from tecdoc_etl.config import SCHEMA, Settings
from tecdoc_etl.db import (
    connect,
    delete_tecdoc_rows_for_dlnr,
    ensure_etl_schema,
    list_schema_tables,
    ping_db,
    truncate_all_tecdoc_tables,
)
from tecdoc_etl.extract import extract_7z
from tecdoc_etl.jobs import append_error, create_job, get_job, update_job
from tecdoc_etl.loader import get_copy_column_names, load_file_into_table
from tecdoc_etl.sftp import download_taf_archives, list_remote_archives

logger = logging.getLogger(__name__)

DATA_FILE_RE = re.compile(r"^(\d{3})\.(\d{4})$")

LOCAL_ARCHIVE_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+\.7z$", re.IGNORECASE)

# Single-segment folder name under DATA_DIR (e.g. 0001 for data/0001)
DATA_FOLDER_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+$")

_sync_busy = threading.Lock()
_running_flag = False


def sync_busy() -> bool:
    with _sync_busy:
        return _running_flag


def _set_running(v: bool) -> None:
    global _running_flag
    with _sync_busy:
        _running_flag = v


def discover_data_files(extract_root: Path) -> dict[str, list[Path]]:
    """Map TecDoc table name (t001) -> ordered list of data file paths."""
    by_table: dict[str, list[tuple[int, Path]]] = {}
    for dirpath, _, filenames in os.walk(extract_root):
        for fn in filenames:
            m = DATA_FILE_RE.match(fn)
            if not m:
                continue
            prefix, suffix = m.group(1), m.group(2)
            table = f"t{prefix}"
            by_table.setdefault(table, []).append((int(suffix), Path(dirpath) / fn))
    result: dict[str, list[Path]] = {}
    for t, items in by_table.items():
        items.sort(key=lambda x: x[0])
        result[t] = [p for _, p in items]
    return result


def resolve_local_archive_basename(settings: Settings, filename: str) -> Path:
    """
    Resolve a .7z basename under settings.data_dir only (no path traversal).
    Raises ValueError if name invalid; FileNotFoundError if missing.
    """
    name = filename.strip()
    if not name or not LOCAL_ARCHIVE_NAME_RE.match(name):
        raise ValueError("Invalid archive name")
    if skip_logistics_supplier_id(name):
        raise ValueError(f"Logistics archives are skipped: {name}")
    data_dir = Path(settings.data_dir).resolve()
    candidate = (data_dir / name).resolve()
    try:
        candidate.relative_to(data_dir)
    except ValueError as e:
        raise ValueError("Path escapes data directory") from e
    if not candidate.is_file():
        raise FileNotFoundError(f"Archive not found: {name}")
    if candidate.suffix.lower() != ".7z":
        raise ValueError("Not a .7z file")
    return candidate


def list_local_archives(settings: Settings) -> list[str]:
    """Basenames of *.7z files directly under data_dir (non-recursive)."""
    data_dir = Path(settings.data_dir)
    if not data_dir.is_dir():
        return []
    names = sorted(
        p.name
        for p in data_dir.iterdir()
        if p.is_file()
        and p.suffix.lower() == ".7z"
        and not skip_logistics_supplier_id(p.name)
    )
    return names


def resolve_data_folder_basename(settings: Settings, name: str) -> Path:
    """
    Resolve a single directory name under settings.data_dir (e.g. '0001' -> data/0001).
    No path separators. Raises ValueError or FileNotFoundError.
    """
    name = name.strip()
    if not name or not DATA_FOLDER_NAME_RE.match(name) or "/" in name or name in (".", ".."):
        raise ValueError("Invalid folder name")
    data_dir = Path(settings.data_dir).resolve()
    candidate = (data_dir / name).resolve()
    try:
        candidate.relative_to(data_dir)
    except ValueError as e:
        raise ValueError("Path escapes data directory") from e
    if not candidate.is_dir():
        raise FileNotFoundError(f"Folder not found: {name}")
    return candidate


def list_data_folders(settings: Settings) -> list[str]:
    """Immediate subdirectories of data_dir (non-recursive), sorted."""
    data_dir = Path(settings.data_dir)
    if not data_dir.is_dir():
        return []
    return sorted(p.name for p in data_dir.iterdir() if p.is_dir())


def _load_from_extract_dirs(
    conn: psycopg2.extensions.connection,
    settings: Settings,
    job_id: int,
    extract_dirs: list[Path],
    progress: dict[str, Any],
    errors: list[dict[str, Any]],
    log: Callable[[str], None],
    *,
    truncate_all: bool | None = None,
    finalize_job: bool = True,
) -> dict[str, Any]:
    """
    Merge discovery from extract roots, optional truncate, COPY into tables.

    ``truncate_all``: when ``True``, truncate every ``tecdoc_gheorghe`` table before load; when
    ``False``, append only; when ``None``, follow ``settings.sync_truncate``.

    ``finalize_job``: when ``False``, do not set job status to completed/failed or ``finished_at``
    (used by multi-supplier D_TAF batch loads).
    """
    steps: list[dict[str, Any]] = progress.setdefault("steps", [])

    def step(msg: str) -> None:
        steps.append({"message": msg})
        log(msg)
        update_job(conn, job_id, progress=progress)

    layout_note = (
        "fixed-width from TecDoc column positions CSV"
        if settings.use_tecdoc_csv_positions and settings.row_parse_mode == "fixed_width"
        else f"ROW_PARSE_MODE={settings.row_parse_mode}"
    )
    step(
        f"Scanning {len(extract_dirs)} path(s) for TecDoc files (NNN.xxxx; {layout_note}) …"
    )
    merged: dict[str, list[Path]] = {}
    for root in extract_dirs:
        step(f"Scanning folder: {root}")
        part = discover_data_files(root)
        n_files = sum(len(v) for v in part.values())
        step(f"  Found {len(part)} table code(s), {n_files} data file(s) under {root.name or root}")
        for t, paths in part.items():
            merged.setdefault(t, []).extend(paths)
    for t in merged:
        merged[t].sort(key=lambda p: p.name)

    file_total = sum(len(paths) for paths in merged.values())
    step(f"Discovery complete: {len(merged)} distinct table group(s), {file_total} file(s) total.")

    known_tables = set(list_schema_tables(conn))
    to_load: dict[str, list[Path]] = {}
    for t, paths in sorted(merged.items()):
        if t not in known_tables:
            progress["skipped_tables"].append(
                {"table": t, "files": [p.name for p in paths]}
            )
            step(f"Skipping unknown schema table {t} ({len(paths)} file(s): {', '.join(p.name for p in paths[:5])}{'…' if len(paths) > 5 else ''})")
            if settings.strict_tables:
                raise RuntimeError(f"Strict mode: table {t} not in schema {SCHEMA}")
            continue
        to_load[t] = paths

    if not to_load:
        msg = "No data files matched existing tables."
        progress["total_rows"] = 0
        progress["phase"] = "no_files"
        if finalize_job:
            errors.append({"level": "error", "message": msg})
            step(msg)
            update_job(
                conn,
                job_id,
                status="failed",
                message=msg,
                progress=progress,
                errors=errors,
                finished=True,
            )
        else:
            errors.append({"level": "warning", "message": msg, "scope": "supplier_chunk"})
            step(msg)
            update_job(conn, job_id, progress=progress, errors=errors)
        return get_job(conn, job_id)  # type: ignore[return-value]

    step(
        f"Will load {len(to_load)} table(s) into {SCHEMA} ({layout_note}): "
        f"{', '.join(sorted(to_load.keys())[:20])}{'…' if len(to_load) > 20 else ''}"
    )

    do_truncate = settings.sync_truncate if truncate_all is None else truncate_all
    if do_truncate:
        progress["phase"] = "truncate"
        step("Truncating all tables in tecdoc_gheorghe …")
        truncate_all_tecdoc_tables(conn)
        step("Truncate finished.")
        update_job(conn, job_id, progress=progress)
    else:
        step("Skipping full truncate: appending rows (per-supplier loads use DELETE by dlnr first).")

    progress["phase"] = "load"
    total_rows = 0
    for table in sorted(to_load.keys()):
        try:
            cols = get_copy_column_names(conn, settings, table)
        except Exception as e:
            err = {"table": table, "message": f"column layout: {e}"}
            errors.append(err)
            append_error(conn, job_id, err, current_errors=errors)
            step(f"ERROR: {table} column layout failed: {e}")
            continue
        if not cols:
            err = {"table": table, "message": "no columns (table missing?)"}
            errors.append(err)
            append_error(conn, job_id, err, current_errors=errors)
            step(f"ERROR: {table} has no columns in DB, skipping.")
            continue
        step(
            f"Table {table}: {len(cols)} COPY column(s), {len(to_load[table])} file(s) to import."
        )
        table_rows = 0
        for fp in to_load[table]:
            if settings.row_parse_mode == "delimiter":
                parse_hint = f"delimiter={settings.field_delimiter!r}"
            elif settings.use_tecdoc_csv_positions:
                parse_hint = (
                    f"TecDoc column positions CSV ({settings.tecdoc_column_positions_csv.name})"
                )
            else:
                parse_hint = "fixed-width from varchar(n) / integer / numeric widths"
            step(f"Loading {SCHEMA}.{table} ← file {fp.name} ({parse_hint}) …")
            try:
                n = load_file_into_table(
                    conn,
                    settings,
                    table,
                    cols,
                    fp,
                    log_prefix=f"[{table}] ",
                )
                table_rows += n
                step(
                    f"  OK {fp.name}: {n} row(s) copied into {SCHEMA}.{table} (table subtotal {table_rows})."
                )
            except Exception as e:
                logger.exception("Load failed for %s", fp)
                err = {"table": table, "file": str(fp.name), "message": str(e)}
                errors.append(err)
                append_error(conn, job_id, err, current_errors=errors)
                step(f"  FAILED {fp.name}: {e}")
                raise
        progress["tables"][table] = table_rows
        total_rows += table_rows
        step(f"Table {table} done: {table_rows} row(s) total for this table.")
        update_job(conn, job_id, progress=progress)

    progress["phase"] = "done"
    progress["total_rows"] = total_rows
    if finalize_job:
        update_job(
            conn,
            job_id,
            status="completed",
            message=f"Loaded {total_rows} rows.",
            progress=progress,
            errors=errors,
            finished=True,
        )
    else:
        update_job(conn, job_id, progress=progress, errors=errors, message=f"Chunk: {total_rows} rows.")
    return get_job(conn, job_id)  # type: ignore[return-value]


def run_sync(
    settings: Settings,
    *,
    dry_run: bool = False,
    job_id: int | None = None,
    log_cb: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """
    Full pipeline: optional SFTP download, extract 7z, COPY into tecdoc_gheorghe.
    If job_id is None, creates a new job row (caller must pass conn).
    """
    def log(msg: str) -> None:
        logger.info(msg)
        if log_cb:
            log_cb(msg)

    conn = connect(settings)
    ensure_etl_schema(conn)
    if not ping_db(conn):
        raise RuntimeError("Database ping failed")

    errors: list[dict[str, Any]] = []
    progress: dict[str, Any] = {
        "phase": "init",
        "source": "sftp",
        "archives": [],
        "tables": {},
        "skipped_tables": [],
        "steps": [],
    }

    own_job = job_id is None
    if own_job:
        job_id = create_job(conn, dry_run=dry_run)
    else:
        update_job(conn, job_id, progress=progress)

    try:
        conn.set_client_encoding("UTF8")

        data_dir = Path(settings.data_dir)
        dl = data_dir / "downloads"
        ex = data_dir / "extract"
        dl.mkdir(parents=True, exist_ok=True)
        ex.mkdir(parents=True, exist_ok=True)

        if dry_run:
            log("Dry run: listing remote archives only (no download/extract/load).")
            progress["phase"] = "dry_run"
            names = list_remote_archives(settings)
            progress["remote_archives"] = names
            update_job(conn, job_id, progress=progress, status="completed", finished=True)
            return get_job(conn, job_id)  # type: ignore

        progress["phase"] = "download"
        update_job(conn, job_id, progress=progress)

        archives = download_taf_archives(
            settings,
            dl,
            on_progress=log,
        )
        progress["archives"] = [str(p.name) for p in archives]
        update_job(conn, job_id, progress=progress)

        if not archives:
            msg = "No matching archives found on SFTP."
            log(msg)
            errors.append({"level": "warning", "message": msg})
            update_job(
                conn,
                job_id,
                status="completed",
                message=msg,
                progress=progress,
                errors=errors,
                finished=True,
            )
            return get_job(conn, job_id)  # type: ignore

        progress["phase"] = "extract"
        extract_dirs: list[Path] = []
        for arc in archives:
            sub = ex / arc.stem
            sub.mkdir(parents=True, exist_ok=True)
            extract_7z(arc, sub, on_progress=log)
            extract_dirs.append(sub)

        return _load_from_extract_dirs(
            conn,
            settings,
            job_id,
            extract_dirs,
            progress,
            errors,
            log,
        )

    except Exception as e:
        logger.exception("Sync failed")
        errors.append({"level": "error", "message": str(e)})
        update_job(
            conn,
            job_id,
            status="failed",
            message=str(e),
            progress=progress,
            errors=errors,
            finished=True,
        )
        raise
    finally:
        conn.close()


def run_local_archive_sync(
    settings: Settings,
    archive_path: Path,
    *,
    job_id: int | None = None,
    log_cb: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """
    Extract one local .7z under data_dir and load all NNN.xxxx files into tables.
    """
    def log(msg: str) -> None:
        logger.info(msg)
        if log_cb:
            log_cb(msg)

    if not archive_path.is_file():
        raise FileNotFoundError(f"Not a file: {archive_path}")
    if archive_path.suffix.lower() != ".7z":
        raise ValueError("Expected a .7z archive")
    if skip_logistics_supplier_id(archive_path.name):
        raise ValueError(f"Logistics archives are skipped: {archive_path.name}")

    conn = connect(settings)
    ensure_etl_schema(conn)
    if not ping_db(conn):
        raise RuntimeError("Database ping failed")

    errors: list[dict[str, Any]] = []
    progress: dict[str, Any] = {
        "phase": "init",
        "source": "local",
        "archive": archive_path.name,
        "archives": [archive_path.name],
        "tables": {},
        "skipped_tables": [],
        "steps": [],
    }

    own_job = job_id is None
    if own_job:
        job_id = create_job(conn, dry_run=False)
    else:
        update_job(conn, job_id, progress=progress)

    try:
        conn.set_client_encoding("UTF8")

        data_dir = Path(settings.data_dir)
        ex = data_dir / "extract"
        ex.mkdir(parents=True, exist_ok=True)

        progress["phase"] = "extract"
        update_job(conn, job_id, progress=progress)
        extract_root = ex / archive_path.stem
        extract_root.mkdir(parents=True, exist_ok=True)
        extract_7z(archive_path, extract_root, on_progress=log)

        return _load_from_extract_dirs(
            conn,
            settings,
            job_id,
            [extract_root],
            progress,
            errors,
            log,
        )

    except Exception as e:
        logger.exception("Local archive sync failed")
        errors.append({"level": "error", "message": str(e)})
        update_job(
            conn,
            job_id,
            status="failed",
            message=str(e),
            progress=progress,
            errors=errors,
            finished=True,
        )
        raise
    finally:
        conn.close()


def run_sync_task(settings: Settings, job_id: int, dry_run: bool) -> None:
    """Wrapper for background jobs: manages busy flag."""
    _set_running(True)
    try:
        run_sync(settings, dry_run=dry_run, job_id=job_id)
    finally:
        _set_running(False)


def run_local_archive_task(
    settings: Settings, job_id: int, archive_path: Path
) -> None:
    _set_running(True)
    try:
        run_local_archive_sync(settings, archive_path, job_id=job_id)
    finally:
        _set_running(False)


def run_folder_load_sync(
    settings: Settings,
    folder_path: Path,
    *,
    job_id: int | None = None,
    log_cb: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """
    Load TecDoc flat files from an existing folder tree (e.g. data/0001/001.0001)
    into tecdoc_gheorghe. For fixed-width mode, uses documentation column positions CSV
    (TecDoc PDF Pos/Length) when available; otherwise schema-derived widths. No extraction step.
    """
    def log(msg: str) -> None:
        logger.info(msg)
        if log_cb:
            log_cb(msg)

    if not folder_path.is_dir():
        raise FileNotFoundError(f"Not a directory: {folder_path}")

    conn = connect(settings)
    ensure_etl_schema(conn)
    if not ping_db(conn):
        raise RuntimeError("Database ping failed")

    errors: list[dict[str, Any]] = []
    progress: dict[str, Any] = {
        "phase": "init",
        "source": "folder",
        "folder": folder_path.name,
        "path": str(folder_path),
        "tables": {},
        "skipped_tables": [],
        "steps": [],
    }

    own_job = job_id is None
    if own_job:
        job_id = create_job(conn, dry_run=False)
    else:
        update_job(conn, job_id, progress=progress)

    try:
        conn.set_client_encoding("UTF8")
        start_msg = (
            f"Starting folder load: {folder_path} → schema {SCHEMA} "
            f"(ROW_PARSE_MODE={settings.row_parse_mode}"
            + (
                f", FIELD_DELIMITER={settings.field_delimiter!r})"
                if settings.row_parse_mode == "delimiter"
                else (
                    f", fixed-width via {settings.tecdoc_column_positions_csv.name}"
                    f" when USE_TECDOC_CSV_POSITIONS=1 and table is mapped, else schema widths)"
                )
            )
        )
        progress["steps"].append({"message": start_msg})
        log(start_msg)
        progress["phase"] = "load"
        update_job(conn, job_id, progress=progress)

        return _load_from_extract_dirs(
            conn,
            settings,
            job_id,
            [folder_path],
            progress,
            errors,
            log,
        )

    except Exception as e:
        logger.exception("Folder load failed")
        errors.append({"level": "error", "message": str(e)})
        update_job(
            conn,
            job_id,
            status="failed",
            message=str(e),
            progress=progress,
            errors=errors,
            finished=True,
        )
        raise
    finally:
        conn.close()


def run_folder_load_task(
    settings: Settings, job_id: int, folder_path: Path
) -> None:
    _set_running(True)
    try:
        run_folder_load_sync(settings, folder_path, job_id=job_id)
    finally:
        _set_running(False)


def list_d_taf_supplier_folders(settings: Settings) -> list[str]:
    """Sorted supplier ids under D_TAF (from ``list_d_taf_supplier_plan``)."""
    return [e["id"] for e in list_d_taf_supplier_plan(settings)]


def list_d_taf_supplier_plan(settings: Settings) -> list[dict[str, Any]]:
    """
    Build the ordered D_TAF batch plan: ``.7z`` archives and/or per-supplier folders, **ASC** by id.

    Each entry: ``{"id": "0001", "kind": "archive"|"folder", "path": "<absolute path>"}``.
    If both ``0001.7z`` and directory ``0001/`` exist, the **archive** is used (single row per id).

    Suppliers whose id (archive stem or folder name) contains ``_logistics`` (case-insensitive)
    are omitted.
    """
    try:
        root = resolve_d_taf_root(settings)
    except FileNotFoundError:
        return []

    archives: dict[str, Path] = {}
    for p in root.iterdir():
        if not p.is_file() or p.suffix.lower() != ".7z":
            continue
        if not LOCAL_ARCHIVE_NAME_RE.match(p.name):
            continue
        if skip_logistics_supplier_id(p.name):
            continue
        stem = p.stem
        if not DATA_FOLDER_NAME_RE.match(stem) or stem in (".", ".."):
            continue
        archives[stem] = p.resolve()

    folders: dict[str, Path] = {}
    for p in root.iterdir():
        if not p.is_dir():
            continue
        if not DATA_FOLDER_NAME_RE.match(p.name) or p.name in (".", ".."):
            continue
        if skip_logistics_supplier_id(p.name):
            continue
        folders[p.name] = p.resolve()

    keys = sorted(set(archives) | set(folders))
    out: list[dict[str, Any]] = []
    for k in keys:
        if k in archives:
            out.append({"id": k, "kind": "archive", "path": str(archives[k])})
        else:
            out.append({"id": k, "kind": "folder", "path": str(folders[k])})
    return out


def _d_taf_plan_from_progress(prog: dict[str, Any], root: Path) -> list[dict[str, Any]]:
    """Restore per-supplier source from job JSON, or legacy folder-only plan."""
    plan = prog.get("supplier_plan")
    if isinstance(plan, list) and plan:
        return list(plan)
    return [
        {"id": s, "kind": "folder", "path": str(root / s)}
        for s in prog.get("suppliers", [])
    ]


def _filtered_d_taf_plan_and_index(
    raw_plan: list[dict[str, Any]], idx_in_raw: int
) -> tuple[list[dict[str, Any]], int]:
    """
    Drop logistics suppliers from ``raw_plan``. Map ``idx_in_raw`` (index into the unfiltered plan
    as stored on the job) to the starting index in the filtered plan for resume.
    """
    filtered = [e for e in raw_plan if not skip_logistics_supplier_id(str(e.get("id", "")))]
    n_raw = len(raw_plan)
    i = max(0, min(int(idx_in_raw), n_raw))
    c_before = sum(
        1
        for j in range(i)
        if j < n_raw and not skip_logistics_supplier_id(str(raw_plan[j].get("id", "")))
    )
    return filtered, c_before


def _d_taf_require_under_data_dir(settings: Settings, path: Path) -> Path:
    data_dir = Path(settings.data_dir).resolve()
    resolved = path.resolve()
    try:
        resolved.relative_to(data_dir)
    except ValueError as e:
        raise ValueError(f"Path must be under DATA_DIR: {path}") from e
    return resolved


def resolve_d_taf_root(settings: Settings) -> Path:
    """Resolved ``data_dir / d_taf_subdir``; must stay under ``data_dir``."""
    data_dir = Path(settings.data_dir).resolve()
    root = (data_dir / settings.d_taf_subdir).resolve()
    try:
        root.relative_to(data_dir)
    except ValueError as e:
        raise ValueError("D_TAF path escapes data directory") from e
    if not root.is_dir():
        raise FileNotFoundError(
            f"D_TAF directory not found: {data_dir / settings.d_taf_subdir}"
        )
    return root


def run_d_taf_batch_sync(settings: Settings, job_id: int) -> dict[str, Any]:
    """
    Sequentially load each supplier under ``DATA_DIR/D_TAF_SUBDIR`` into ``tecdoc_gheorghe``.

    Sources are ``*.7z`` archives (extracted to ``DATA_DIR/extract/<stem>/``) and/or existing
    per-supplier folders (see ``list_d_taf_supplier_plan``). Ascending order by supplier id; if both
    ``0001.7z`` and ``0001/`` exist, the archive is used.

    For each supplier: delete existing rows for that ``dlnr``, then COPY (no global truncate).
    """
    conn = connect(settings)
    ensure_etl_schema(conn)
    if not ping_db(conn):
        raise RuntimeError("Database ping failed")

    j = get_job(conn, job_id)
    if not j:
        raise ValueError(f"Job {job_id} not found")
    prog: dict[str, Any] = dict(j["progress"])
    if prog.get("kind") != "d_taf_batch":
        raise ValueError("Job progress is not a D_TAF batch (kind != d_taf_batch)")

    errors: list[dict[str, Any]] = list(j.get("errors") or [])
    root = Path(prog["root"])
    raw_plan = _d_taf_plan_from_progress(prog, root)
    idx_raw = int(prog.get("index", 0))
    plan, idx_start = _filtered_d_taf_plan_and_index(raw_plan, idx_raw)
    n_plan = len(plan)
    raw_ids = [str(e.get("id", "")) for e in raw_plan]
    filt_ids = [str(e.get("id", "")) for e in plan]
    if raw_ids != filt_ids:
        prog["supplier_plan"] = plan
        prog["suppliers"] = filt_ids
        prog["index"] = idx_start
        update_job(conn, job_id, progress=prog, errors=errors)

    def log(msg: str) -> None:
        logger.info("[d_taf job=%s] %s", job_id, msg)

    try:
        conn.set_client_encoding("UTF8")
        batch_totals = int(prog.get("batch_total_rows", 0))

        for i in range(idx_start, n_plan):
            entry = plan[i]
            supplier = entry["id"]
            kind = entry.get("kind") or "folder"
            src_path = _d_taf_require_under_data_dir(settings, Path(entry["path"]))

            prog["index"] = i
            prog["current_supplier"] = supplier
            prog["phase"] = "purge_supplier"
            prog.setdefault("steps", []).append(
                {
                    "message": (
                        f"[D_TAF] Supplier {supplier} ({i + 1}/{n_plan}, {kind}): "
                        "purge existing rows for this dlnr …"
                    )
                }
            )
            update_job(conn, job_id, progress=prog, errors=errors)

            log(f"Purging database rows for supplier {supplier!r} ({kind})")
            purge_info = delete_tecdoc_rows_for_dlnr(conn, supplier, log=logger)
            prog.setdefault("purge_log", []).append({"supplier": supplier, **purge_info})
            prog["steps"].append(
                {
                    "message": (
                        f"[D_TAF] Purge {supplier}: "
                        f"{purge_info['rows_deleted_total']} row(s) removed from "
                        f"{purge_info['tables_touched']} table(s) (with FK-safe retries)."
                    )
                }
            )
            update_job(conn, job_id, progress=prog, errors=errors)

            if kind == "archive":
                if not src_path.is_file():
                    err = f"Supplier archive missing: {src_path}"
                    errors.append({"level": "error", "message": err, "supplier": supplier})
                    update_job(
                        conn,
                        job_id,
                        status="failed",
                        message=err,
                        progress=prog,
                        errors=errors,
                        finished=True,
                    )
                    raise FileNotFoundError(err)
                extract_parent = Path(settings.data_dir).resolve() / "extract"
                extract_root = _d_taf_require_under_data_dir(
                    settings, extract_parent / src_path.stem
                )
                prog["phase"] = "extract_supplier"
                prog["steps"].append(
                    {
                        "message": (
                            f"[D_TAF] Supplier {supplier}: extracting {src_path.name} "
                            f"→ {extract_root} …"
                        )
                    }
                )
                update_job(conn, job_id, progress=prog, errors=errors)
                if extract_root.exists():
                    shutil.rmtree(extract_root)
                extract_root.mkdir(parents=True, exist_ok=True)
                extract_7z(src_path, extract_root, on_progress=log)
                prog["steps"].append(
                    {
                        "message": (
                            f"[D_TAF] Supplier {supplier}: extraction finished; loading from "
                            f"{extract_root.name}/"
                        )
                    }
                )
                update_job(conn, job_id, progress=prog, errors=errors)
                load_roots = [extract_root]
            else:
                if not src_path.is_dir():
                    err = f"Supplier folder missing: {src_path}"
                    errors.append({"level": "error", "message": err, "supplier": supplier})
                    update_job(
                        conn,
                        job_id,
                        status="failed",
                        message=err,
                        progress=prog,
                        errors=errors,
                        finished=True,
                    )
                    raise FileNotFoundError(err)
                load_roots = [src_path]

            prog["tables"] = {}
            prog["skipped_tables"] = []
            prog["total_rows"] = 0
            prog["phase"] = "load_supplier"

            _load_from_extract_dirs(
                conn,
                settings,
                job_id,
                load_roots,
                prog,
                errors,
                log,
                truncate_all=False,
                finalize_job=False,
            )

            chunk_rows = int(prog.get("total_rows", 0))
            batch_totals += chunk_rows
            prog["batch_total_rows"] = batch_totals
            by = prog.setdefault("by_supplier", {})
            by[supplier] = {
                "rows": chunk_rows,
                "kind": kind,
                "tables": dict(prog.get("tables", {})),
            }
            comp = prog.setdefault("completed", [])
            if supplier not in comp:
                comp.append(supplier)
            prog["index"] = i + 1
            prog["current_supplier"] = None
            prog["steps"].append(
                {
                    "message": (
                        f"[D_TAF] Supplier {supplier} done: {chunk_rows} row(s) "
                        f"({kind}); batch cumulative {batch_totals}."
                    )
                }
            )
            update_job(conn, job_id, progress=prog, errors=errors)

        prog["phase"] = "done"
        prog["current_supplier"] = None
        update_job(
            conn,
            job_id,
            status="completed",
            message=(
                f"D_TAF batch finished: {n_plan} supplier(s), "
                f"{prog.get('batch_total_rows', 0)} row(s) loaded."
            ),
            progress=prog,
            errors=errors,
            finished=True,
        )
        return get_job(conn, job_id)  # type: ignore[return-value]
    except Exception as e:
        logger.exception("D_TAF batch failed")
        errors.append(
            {
                "level": "error",
                "message": str(e),
                "supplier": prog.get("current_supplier"),
            }
        )
        update_job(
            conn,
            job_id,
            status="failed",
            message=str(e),
            progress=prog,
            errors=errors,
            finished=True,
        )
        raise
    finally:
        conn.close()


def run_d_taf_batch_task(settings: Settings, job_id: int) -> None:
    _set_running(True)
    try:
        run_d_taf_batch_sync(settings, job_id)
    finally:
        _set_running(False)
