"""
Load per-column 0-based slice positions from documentation/tecdoc_column_string_positions.csv
(TecDoc Data Format 2.7 PDF → DB column mapping).

Rules:

- Any row with non-empty ``pos_0based_start`` and ``field_length`` (integers) defines an absolute
  slice; ``mapping_note`` is informational only.

- If a table has **no** absolute slices but every CSV row for that table shares the same
  ``pdf_record_length`` and each column has a derivable width from ``pg_type`` (e.g. ``varchar(5)``,
  ``integer``), **sequential** slices are built in ``db_column_ordinal`` order so the record still
  follows the CSV (PDF length + column list). A single leading/trailing integer width may be
  adjusted to match ``pdf_record_length`` when the varchar sum differs by a few characters
  (e.g. ``t120``).

Used when ``USE_TECDOC_CSV_POSITIONS=1`` (see ``Settings.use_tecdoc_csv_positions``).
Tables with neither absolute positions nor enough metadata still fall back to schema-width parsing
in the loader (with a log line).
"""

from __future__ import annotations

import csv
import logging
import re
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tecdoc_etl.config import Settings

logger = logging.getLogger(__name__)


def _default_csv_path() -> Path:
    return Path(__file__).resolve().parent.parent / "documentation/tecdoc_column_string_positions.csv"


def _width_from_pg_type(
    pg_type: str,
    *,
    int_w: int,
    bigint_w: int,
    smallint_w: int,
) -> int | None:
    """Character width for one physical field from the CSV ``pg_type`` cell."""
    s = (pg_type or "").strip().lower()
    if not s:
        return None
    if s == "integer" or s == "int" or s == "int4":
        return int_w
    if s == "bigint" or s == "int8":
        return bigint_w
    if s == "smallint" or s == "int2":
        return smallint_w
    m = re.match(r"varchar\((\d+)\)", s)
    if m:
        return int(m.group(1))
    if s == "varchar":
        return None
    m = re.match(r"char\((\d+)\)", s)
    if m:
        return int(m.group(1))
    m = re.match(r"numeric\((\d+)\s*,\s*(\d+)\)", s)
    if m:
        return int(m.group(1))
    m = re.match(r"numeric\((\d+)\)", s)
    if m:
        return int(m.group(1))
    m = re.match(r"decimal\((\d+)\s*,\s*(\d+)\)", s)
    if m:
        return int(m.group(1))
    return None


def _is_integer_pg_type(pg_type: str) -> bool:
    s = (pg_type or "").strip().lower()
    return s in ("integer", "int", "int4", "bigint", "int8", "smallint", "int2")


def _try_synthetic_sequential_slices(
    rows: list[dict[str, str]],
    record_len: int,
    *,
    int_w: int,
    bigint_w: int,
    smallint_w: int,
) -> dict[str, tuple[int, int]] | None:
    """
    Build contiguous slices when the CSV lists all columns and ``pdf_record_length`` but no
    ``pos_0based_start`` / ``field_length``.
    """
    if not rows or record_len <= 0:
        return None
    for row in rows:
        p0 = row.get("pos_0based_start", "").strip()
        fl = row.get("field_length", "").strip()
        if p0 and fl:
            try:
                int(p0)
                int(fl)
                return None
            except ValueError:
                return None

    try:
        sorted_rows = sorted(rows, key=lambda r: int(r.get("db_column_ordinal", "0")))
    except ValueError:
        return None

    widths: list[tuple[str, int]] = []
    int_indices: list[int] = []
    for i, row in enumerate(sorted_rows):
        col = (row.get("db_column_name") or "").strip()
        if not col:
            return None
        w = _width_from_pg_type(
            row.get("pg_type", ""),
            int_w=int_w,
            bigint_w=bigint_w,
            smallint_w=smallint_w,
        )
        if w is None:
            return None
        widths.append((col, w))
        if _is_integer_pg_type(row.get("pg_type", "")):
            int_indices.append(i)

    total = sum(w for _, w in widths)
    delta = record_len - total
    if delta != 0:
        if len(int_indices) != 1:
            logger.warning(
                "Synthetic CSV layout: record_len=%s vs width_sum=%s (delta=%s); "
                "need exactly one integer column to adjust, skipping synthetic layout",
                record_len,
                total,
                delta,
            )
            return None
        idx = int_indices[0]
        col, w0 = widths[idx]
        w1 = w0 + delta
        if w1 < 1:
            return None
        widths[idx] = (col, w1)

    out: dict[str, tuple[int, int]] = {}
    pos = 0
    for col, ln in widths:
        out[col] = (pos, ln)
        pos += ln
    return out


@lru_cache(maxsize=32)
def _load_maps_cached(
    path_str: str,
    mtime_ns: int,
    int_w: int,
    bigint_w: int,
    smallint_w: int,
) -> tuple[dict[str, dict[str, tuple[int, int]]], dict[str, int]]:
    path = Path(path_str)
    by_table: dict[str, dict[str, tuple[int, int]]] = {}
    record_len: dict[str, int] = {}
    rows_by_table: dict[str, list[dict[str, str]]] = defaultdict(list)

    with path.open(encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            t = row.get("table_name", "").strip().lower()
            if not t:
                continue
            rows_by_table[t].append(row)
            prl = row.get("pdf_record_length", "").strip()
            if prl.isdigit():
                record_len[t] = max(record_len.get(t, 0), int(prl))

            col = row.get("db_column_name", "").strip()
            p0 = row.get("pos_0based_start", "").strip()
            fl = row.get("field_length", "").strip()
            if not col or not p0 or not fl:
                continue
            try:
                start = int(p0)
                length = int(fl)
            except ValueError:
                continue
            if length <= 0:
                continue
            by_table.setdefault(t, {})[col] = (start, length)

    for t, rows in rows_by_table.items():
        if by_table.get(t):
            continue
        R = record_len.get(t)
        if not R:
            continue
        synth = _try_synthetic_sequential_slices(
            rows,
            R,
            int_w=int_w,
            bigint_w=bigint_w,
            smallint_w=smallint_w,
        )
        if synth:
            by_table[t] = synth
            logger.info(
                "Built sequential TecDoc CSV slices for %s from pg_type + pdf_record_length=%s",
                t,
                R,
            )

    return by_table, record_len


def load_column_position_maps(
    csv_path: Path | None,
    settings: Settings | None = None,
) -> tuple[dict[str, dict[str, tuple[int, int]]], dict[str, int]]:
    """Return (table -> {column_name: (start_0based, length)}, table -> pdf_record_length)."""
    path = csv_path if csv_path is not None else _default_csv_path()
    if not path.is_file():
        return {}, {}
    int_w = getattr(settings, "fixed_width_integer_width", None) or 9
    bigint_w = getattr(settings, "fixed_width_bigint_width", None) or 18
    smallint_w = getattr(settings, "fixed_width_smallint_width", None) or 5
    try:
        st = path.stat()
        return _load_maps_cached(
            str(path.resolve()),
            int(st.st_mtime_ns),
            int_w,
            bigint_w,
            smallint_w,
        )
    except OSError as e:
        logger.warning("Could not read column positions CSV %s: %s", path, e)
        return {}, {}


def slices_for_table(
    csv_path: Path | None,
    table_name: str,
    settings: Settings | None = None,
) -> tuple[dict[str, tuple[int, int]], int | None]:
    """
    Slices and minimum physical line length for one table from the TecDoc column positions CSV.

    Uses absolute ``pos_0based_start`` / ``field_length`` when present; otherwise sequential
    layout derived from ``pdf_record_length`` and ``pg_type`` for that table's rows.
    """
    maps, record_lens = load_column_position_maps(csv_path, settings)
    t = table_name.lower()
    slices = maps.get(t, {})
    if not slices:
        return {}, None
    exp = record_lens.get(t)
    max_end = max(s + ln for s, ln in slices.values())
    if exp is None:
        exp = max_end
    else:
        exp = max(exp, max_end)
    return slices, exp
