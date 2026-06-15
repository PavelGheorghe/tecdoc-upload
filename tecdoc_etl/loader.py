import logging
import os
import tempfile
from pathlib import Path

import psycopg2.extensions
from psycopg2 import sql

from tecdoc_etl.config import SCHEMA, Settings
from tecdoc_etl.db import get_primary_key_columns, get_table_column_load_meta
from tecdoc_etl.column_positions import slices_for_table
from tecdoc_etl.line_layout import (
    T030_RECORD_MIN_LEN,
    build_fixed_width_plan,
    force_empty_string_columns,
    line_to_copy_fields_csv_layout,
    line_to_copy_fields_fixed,
)
from tecdoc_etl.parser import parse_delimited_row, row_to_copy_line

logger = logging.getLogger(__name__)


def _not_exists_insert_sql(
    conn: psycopg2.extensions.connection,
    table_name: str,
    col_sql: sql.Composable,
    tmp_table: str,
    mapped_cols: list[str],
) -> sql.Composable:
    """
    Build INSERT ... WHERE NOT EXISTS SQL.

    Check cols: PK columns when the table has a PK (prevents inserting a row whose key already
    exists regardless of other field values). Falls back to all ``mapped_cols`` for tables with
    no PK (ensures no exact duplicate row on the loaded fields).
    """
    pk_cols = get_primary_key_columns(conn, table_name)
    check_cols = pk_cols if pk_cols else mapped_cols
    where_parts = sql.SQL(" AND ").join(
        sql.SQL("{}.{} = {}.{}").format(
            sql.Identifier("_m"), sql.Identifier(c),
            sql.Identifier("_t"), sql.Identifier(c),
        )
        for c in check_cols
    )
    return sql.SQL(
        "INSERT INTO {schema}.{tbl} ({cols}) "
        "SELECT {cols} FROM {tmp} _t "
        "WHERE NOT EXISTS (SELECT 1 FROM {schema}.{tbl} _m WHERE {where})"
    ).format(
        schema=sql.Identifier(SCHEMA),
        tbl=sql.Identifier(table_name),
        cols=col_sql,
        tmp=sql.Identifier(tmp_table),
        where=where_parts,
    )


def _log_extracted_line(
    settings: Settings,
    log_prefix: str,
    file_path: Path,
    line_no: int,
    text: str,
) -> None:
    """Log the raw line extracted from the file (see LOG_EXTRACTED_LINES)."""
    lim = settings.log_extracted_lines
    if lim == 0:
        return
    if lim == -1:
        logger.debug("%s%s record %s: %r", log_prefix, file_path, line_no, text)
        return
    if line_no <= lim:
        logger.info("%s%s record %s: %r", log_prefix, file_path, line_no, text)


def load_file_into_table(
    conn: psycopg2.extensions.connection,
    settings: Settings,
    table_name: str,
    column_names: list[str],
    file_path: Path,
    *,
    log_prefix: str = "",
    insert_if_not_exists: bool = False,
) -> int:
    """Stream-transform source lines to PostgreSQL COPY text format and COPY into table.

    When ``insert_if_not_exists=True``, rows are COPYed into a temp table then inserted into
    the target with ON CONFLICT DO NOTHING, skipping any row that conflicts with an existing
    primary key or unique constraint.
    """
    if settings.row_parse_mode == "delimiter":
        return _load_delimited(
            conn, settings, table_name, column_names, file_path,
            log_prefix=log_prefix, insert_if_not_exists=insert_if_not_exists,
        )
    return _load_fixed_width(
        conn,
        settings,
        table_name,
        file_path,
        log_prefix=log_prefix,
        insert_if_not_exists=insert_if_not_exists,
    )


def _load_delimited(
    conn: psycopg2.extensions.connection,
    settings: Settings,
    table_name: str,
    column_names: list[str],
    file_path: Path,
    *,
    log_prefix: str,
    insert_if_not_exists: bool = False,
) -> int:
    col_count = len(column_names)
    if col_count == 0:
        raise ValueError(f"No columns for {table_name}")

    col_sql = sql.SQL(", ").join(sql.Identifier(c) for c in column_names)
    tmp_table = f"_tecdoc_ins_{table_name}"

    if insert_if_not_exists:
        copy_stmt = sql.SQL(
            "COPY {} ({}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
        ).format(sql.Identifier(tmp_table), col_sql)
    else:
        copy_stmt = sql.SQL(
            "COPY {}.{} ({}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
        ).format(sql.Identifier(SCHEMA), sql.Identifier(table_name), col_sql)

    delimiter = settings.field_delimiter
    encoding = settings.text_encoding
    empty_as_null = settings.empty_as_null
    force_empty = force_empty_string_columns(table_name)
    never_null_indices = frozenset(
        i for i, c in enumerate(column_names) if c in force_empty
    )
    rows = 0
    bad_rows = 0

    fd, tmp_path = tempfile.mkstemp(prefix="tecdoc_copy_", suffix=".txt", text=True)
    os.close(fd)
    try:
        with open(file_path, "r", encoding=encoding, errors="replace") as src, open(
            tmp_path, "w", encoding="utf-8", newline=""
        ) as dst:
            record_no = 0
            for line in src:
                line = line.rstrip("\r\n")
                if not line.strip():
                    continue
                record_no += 1
                _log_extracted_line(settings, log_prefix, file_path, record_no, line)
                fields = parse_delimited_row(line, delimiter)
                if len(fields) != col_count:
                    bad_rows += 1
                    if bad_rows <= 5:
                        logger.warning(
                            "%sfield count mismatch in %s: expected %s got %s (line sample: %r)",
                            log_prefix,
                            file_path,
                            col_count,
                            len(fields),
                            line[:200],
                        )
                    continue
                dst.write(
                    row_to_copy_line(
                        fields, col_count, empty_as_null, never_null_indices
                    )
                )
                rows += 1

        inserted_count: int | None = None
        try:
            with open(tmp_path, "r", encoding="utf-8") as f:
                with conn.cursor() as cur:
                    if insert_if_not_exists:
                        cur.execute(
                            sql.SQL(
                                "CREATE TEMP TABLE IF NOT EXISTS {} AS "
                                "SELECT {} FROM {}.{} WHERE false"
                            ).format(
                                sql.Identifier(tmp_table),
                                col_sql,
                                sql.Identifier(SCHEMA),
                                sql.Identifier(table_name),
                            )
                        )
                        cur.copy_expert(copy_stmt, f)
                        cur.execute(_not_exists_insert_sql(conn, table_name, col_sql, tmp_table, column_names))
                        inserted_count = cur.rowcount
                        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(tmp_table)))
                    else:
                        cur.copy_expert(copy_stmt, f)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    if bad_rows:
        logger.warning(
            "%sskipped %s rows with field mismatch in %s",
            log_prefix,
            bad_rows,
            file_path,
        )
    return inserted_count if inserted_count is not None else rows


def _load_fixed_width(
    conn: psycopg2.extensions.connection,
    settings: Settings,
    table_name: str,
    file_path: Path,
    *,
    log_prefix: str,
    insert_if_not_exists: bool = False,
) -> int:
    all_meta = get_table_column_load_meta(conn, table_name)
    copy_meta, parse_sequence, widths = build_fixed_width_plan(
        table_name, all_meta, settings
    )
    column_names = [m.name for m in copy_meta]
    col_count = len(column_names)
    if col_count == 0:
        raise ValueError(f"No columns for {table_name}")

    csv_slices: dict[str, tuple[int, int]] | None = None
    csv_min_len: int | None = None
    s, mlen = slices_for_table(
        settings.tecdoc_column_positions_csv, table_name, settings
    )
    if s and mlen is not None:
        # t030: default to TecDoc PDF 102-byte layout from CSV (reserviert 0–22, then dlnr, sa,
        # beznr, sprachnr, bez, lflag) with no lstrip — see line_to_copy_fields_csv_layout.
        t030_pdf_csv = (
            table_name == "t030" and not settings.t030_use_supplier_layout
        )
        use_csv = settings.use_tecdoc_csv_positions or t030_pdf_csv
        if use_csv:
            csv_slices, csv_min_len = s, mlen
    elif settings.use_tecdoc_csv_positions:
        logger.info(
            "%sno TecDoc CSV positions for %s (file=%s); using schema widths",
            log_prefix,
            table_name,
            settings.tecdoc_column_positions_csv,
        )

    col_sql = sql.SQL(", ").join(sql.Identifier(c) for c in column_names)
    tmp_table = f"_tecdoc_ins_{table_name}"

    # Only use the temp-table + ON CONFLICT path when CSV column positions are active for this
    # table — those are the directly-mapped columns from tecdoc_column_string_positions.csv.
    # Schema-derived widths may include DB-only columns (e.g. lflag) that have no position
    # in the spec, so we fall back to a plain COPY in that case.
    use_insert_if_not_exists = insert_if_not_exists and csv_slices is not None

    if use_insert_if_not_exists:
        copy_stmt = sql.SQL(
            "COPY {} ({}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
        ).format(sql.Identifier(tmp_table), col_sql)
    else:
        copy_stmt = sql.SQL(
            "COPY {}.{} ({}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
        ).format(sql.Identifier(SCHEMA), sql.Identifier(table_name), col_sql)

    encoding = settings.text_encoding
    empty_as_null = settings.empty_as_null
    force_empty = force_empty_string_columns(table_name)
    never_null_indices = frozenset(
        i for i, c in enumerate(column_names) if c in force_empty
    )
    rows = 0
    bad_rows = 0
    expected_len = sum(widths)
    len_hint = (
        f">={csv_min_len} (TecDoc column positions CSV)"
        if csv_min_len is not None
        else (
            f">={T030_RECORD_MIN_LEN} (t030 supplier; T030_USE_SUPPLIER_LAYOUT=1)"
            if table_name == "t030"
            else str(expected_len)
        )
    )

    t040_seen_pk: set[tuple[str, str]] | None = None
    t040_ia: int | None = None
    t040_idl: int | None = None
    if table_name == "t040":
        try:
            t040_ia = column_names.index("adressart")
            t040_idl = column_names.index("dlnr")
            t040_seen_pk = set()
        except ValueError:
            t040_ia = t040_idl = None

    fd, tmp_path = tempfile.mkstemp(prefix="tecdoc_copy_", suffix=".txt", text=True)
    os.close(fd)
    dup_t040 = 0
    try:
        with open(file_path, "r", encoding=encoding, errors="replace") as src, open(
            tmp_path, "w", encoding="utf-8", newline=""
        ) as dst:
            record_no = 0
            for line in src:
                raw = line.rstrip("\r\n")
                if not raw.strip():
                    continue
                record_no += 1
                _log_extracted_line(settings, log_prefix, file_path, record_no, raw)
                if csv_slices is not None and csv_min_len is not None:
                    fields = line_to_copy_fields_csv_layout(
                        raw,
                        table_name,
                        copy_meta,
                        csv_slices,
                        csv_min_len,
                    )
                else:
                    fields = line_to_copy_fields_fixed(
                        raw,
                        table_name,
                        copy_meta,
                        parse_sequence,
                        widths,
                        t030_layout=settings.t030_layout,
                    )
                if fields is None:
                    bad_rows += 1
                    if bad_rows <= 5:
                        logger.warning(
                            "%sfixed-width parse skip in %s: raw_len=%s expected=%s sample=%r",
                            log_prefix,
                            file_path,
                            len(raw),
                            len_hint,
                            raw[:120],
                        )
                    continue
                if t040_seen_pk is not None and t040_ia is not None and t040_idl is not None:
                    pk = (fields[t040_ia], fields[t040_idl])
                    if pk in t040_seen_pk:
                        dup_t040 += 1
                        if dup_t040 <= 5:
                            logger.warning(
                                "%sskipping duplicate t040 PK (adressart, dlnr)=%s in %s "
                                "source line %s (keep first row per file)",
                                log_prefix,
                                pk,
                                file_path,
                                record_no,
                            )
                        continue
                    t040_seen_pk.add(pk)
                dst.write(
                    row_to_copy_line(
                        fields, col_count, empty_as_null, never_null_indices
                    )
                )
                rows += 1

        inserted_count: int | None = None
        try:
            with open(tmp_path, "r", encoding="utf-8") as f:
                with conn.cursor() as cur:
                    if use_insert_if_not_exists:
                        cur.execute(
                            sql.SQL(
                                "CREATE TEMP TABLE IF NOT EXISTS {} AS "
                                "SELECT {} FROM {}.{} WHERE false"
                            ).format(
                                sql.Identifier(tmp_table),
                                col_sql,
                                sql.Identifier(SCHEMA),
                                sql.Identifier(table_name),
                            )
                        )
                        cur.copy_expert(copy_stmt, f)
                        # mapped_cols: CSV-positioned columns only — excludes unmapped
                        # columns (e.g. artnr_raw) that are always NULL after insert.
                        mapped_cols = [c for c in column_names if c in csv_slices]
                        cur.execute(_not_exists_insert_sql(conn, table_name, col_sql, tmp_table, mapped_cols))
                        inserted_count = cur.rowcount
                        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(tmp_table)))
                    else:
                        cur.copy_expert(copy_stmt, f)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    if bad_rows:
        logger.warning(
            "%sskipped %s row(s) (length/layout) in %s",
            log_prefix,
            bad_rows,
            file_path,
        )
    if table_name == "t040" and dup_t040:
        logger.warning(
            "%sskipped %s duplicate t040 primary key row(s) in %s (first row kept per key)",
            log_prefix,
            dup_t040,
            file_path,
        )
    return inserted_count if inserted_count is not None else rows


def get_copy_column_names(
    conn: psycopg2.extensions.connection,
    settings: Settings,
    table_name: str,
) -> list[str]:
    """Column list for logging / COPY: matches loader order for the active parse mode."""
    meta = get_table_column_load_meta(conn, table_name)
    if settings.row_parse_mode == "delimiter":
        return [m.name for m in meta if m.udt_name != "tsvector"]
    copy_meta, _, _ = build_fixed_width_plan(table_name, meta, settings)
    return [m.name for m in copy_meta]
