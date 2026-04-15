import logging
import os
import tempfile
from pathlib import Path

import psycopg2.extensions
from psycopg2 import sql

from tecdoc_etl.config import SCHEMA, Settings
from tecdoc_etl.db import get_table_column_load_meta
from tecdoc_etl.column_positions import slices_for_table
from tecdoc_etl.line_layout import (
    T030_RECORD_MIN_LEN,
    build_fixed_width_plan,
    line_to_copy_fields_csv_layout,
    line_to_copy_fields_fixed,
)
from tecdoc_etl.parser import parse_delimited_row, row_to_copy_line

logger = logging.getLogger(__name__)


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
) -> int:
    """Stream-transform source lines to PostgreSQL COPY text format and COPY into table."""
    if settings.row_parse_mode == "delimiter":
        return _load_delimited(
            conn, settings, table_name, column_names, file_path, log_prefix=log_prefix
        )
    return _load_fixed_width(
        conn,
        settings,
        table_name,
        file_path,
        log_prefix=log_prefix,
    )


def _load_delimited(
    conn: psycopg2.extensions.connection,
    settings: Settings,
    table_name: str,
    column_names: list[str],
    file_path: Path,
    *,
    log_prefix: str,
) -> int:
    col_count = len(column_names)
    if col_count == 0:
        raise ValueError(f"No columns for {table_name}")

    copy_stmt = sql.SQL(
        "COPY {}.{} ({}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
    ).format(
        sql.Identifier(SCHEMA),
        sql.Identifier(table_name),
        sql.SQL(", ").join(sql.Identifier(c) for c in column_names),
    )

    delimiter = settings.field_delimiter
    encoding = settings.text_encoding
    empty_as_null = settings.empty_as_null
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
                dst.write(row_to_copy_line(fields, col_count, empty_as_null))
                rows += 1

        try:
            with open(tmp_path, "r", encoding="utf-8") as f:
                with conn.cursor() as cur:
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
    return rows


def _load_fixed_width(
    conn: psycopg2.extensions.connection,
    settings: Settings,
    table_name: str,
    file_path: Path,
    *,
    log_prefix: str,
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

    copy_stmt = sql.SQL(
        "COPY {}.{} ({}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
    ).format(
        sql.Identifier(SCHEMA),
        sql.Identifier(table_name),
        sql.SQL(", ").join(sql.Identifier(c) for c in column_names),
    )

    encoding = settings.text_encoding
    empty_as_null = settings.empty_as_null
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
                dst.write(row_to_copy_line(fields, col_count, empty_as_null))
                rows += 1

        try:
            with open(tmp_path, "r", encoding="utf-8") as f:
                with conn.cursor() as cur:
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
    return rows


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
