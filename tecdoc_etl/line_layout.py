"""
Fixed-width TecDoc flat lines.

For each column in **DDL order**, we take a **contiguous slice** of the input line.
The slice width in **characters** matches the database definition (``varchar(n)`` → *n*
characters from the string, same order as in ``tecdoc_structure.txt`` / ``CREATE TABLE``).

Example ``tecdoc_gheorghe.t001`` (one physical line, 58 chars, no delimiter)::

    00010012613202603241000501SPIDAN              0426    2.70
    |--|   |--|   |--------| ...
    dlnr   sa     datum
    4      3      8           (then kzvoll 1, khernr 6, marke 20, …)

The last two character positions are ``format`` (3 chars, e.g. ``2.7``) and
``lflag`` (1 char, e.g. ``0``) — visually ``2.70`` is ``2.7`` + ``0``, not a 4-char field.

- ``varchar(n)`` / ``char(n)``: exactly **n** characters (``character_maximum_length``).
- ``integer`` / ``bigint`` / ``smallint``: width from env (see Settings).
- ``numeric(p[,s])``: width **p** from ``numeric_precision``.

After slicing, values are **stripped** of padding spaces before COPY.

Most ``tecdoc_gheorghe.t*`` tables follow the rule above. Exceptions: ``DEFAULT_OMIT_FROM_RAW_LINE``
/ ``EXTRA_OMIT_FROM_RAW_LINE`` (e.g. ``t200``, ``t400``, ``t030``).

**Table 030 (language descriptions)** — by default the loader uses
``documentation/tecdoc_column_string_positions.csv`` for **102-byte PDF** records: slice the line
with **only** ``rstrip`` of newlines — **never** ``lstrip`` — so positions 0–21 are ``reserviert``,
then ``dlnr``, ``sa``, ``beznr``, ``sprachnr``, ``bez``, ``lflag`` (see CSV). Set
``T030_USE_SUPPLIER_LAYOUT=1`` to follow ``USE_TECDOC_CSV_POSITIONS`` only and use
``_line_to_copy_fields_t030`` (**supplier** or **pdf102** via ``T030_LAYOUT``) instead of CSV slices.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from tecdoc_etl.db import ColumnLoadMeta

if TYPE_CHECKING:
    from tecdoc_etl.config import Settings

logger = logging.getLogger(__name__)

# Columns present in DB but not in the raw TecDoc line (by table).
# Values are filled with NULL/empty or computed (see _computed_pk).
DEFAULT_OMIT_FROM_RAW_LINE: dict[str, frozenset[str]] = {
    "t200": frozenset({"artnr_raw"}),  # line has artnr, dlnr, sa, … without artnr_raw block
    "t203": frozenset({"artnr_dlnr_khernr_refnr_sort", "refnr"}),
    "t204": frozenset({"ersatznr_artnr_dlnr", "lkz", "exclude", "ersatznr"}),
    "t209": frozenset({"artnr_dlnr_lkz_eannr"}),
    "t210": frozenset({"artnr_dlnr_sortnr"}),
    "t211": frozenset({"artnr_dlnr_genartnr"}),
    "t232": frozenset({"artnr_dlnr_sortnr"}),
    "t400": frozenset({"artnr_dlnr_genartnr_vknzielart_vknzielnr_lfdnr"}),
}

# Extra columns not present in the physical line (merged with DEFAULT_OMIT per table).
EXTRA_OMIT_FROM_RAW_LINE: dict[str, frozenset[str]] = {
    # Export rows: leading padding, then beznr+sprachnr+dlnr+sa+bez+lflag; DDL "reserviert" is absent.
    "t030": frozenset({"reserviert"}),
}

_T030_PREFIX_LEN = 9 + 3 + 4 + 3  # supplier: beznr, sprachnr, dlnr, sa
_T030_SUFFIX_LEN = 1  # lflag
T030_RECORD_MIN_LEN = _T030_PREFIX_LEN + _T030_SUFFIX_LEN
# TecDoc PDF table 030 — fixed 102 characters (1-based positions in tecdoc_column_string_positions.csv).
T030_PDF_RECORD_LEN = 102


def _line_to_copy_fields_t030_supplier(
    rec: str, copy_columns: list[ColumnLoadMeta]
) -> list[str] | None:
    """Supplier export: beznr+sprachnr+dlnr+sa + variable bez + lflag; no reserviert in line."""
    if len(rec) < T030_RECORD_MIN_LEN:
        logger.warning("t030 line too short: len=%s sample=%r", len(rec), rec[:80])
        return None
    beznr = rec[0:9].strip()
    sprachnr = rec[9:12].strip()
    dlnr = rec[12:16].strip()
    sa = rec[16:19].strip()
    bez = rec[19:-1].strip()
    lflag = rec[-1]
    parsed = {
        "beznr": beznr,
        "sprachnr": sprachnr,
        "dlnr": dlnr,
        "sa": sa,
        "bez": bez,
        "lflag": lflag,
    }
    out: list[str] = []
    for m in copy_columns:
        if m.name in parsed:
            out.append(parsed[m.name])
        else:
            out.append("")
    return out


def _line_to_copy_fields_t030_pdf102(
    rec: str, copy_columns: list[ColumnLoadMeta]
) -> list[str] | None:
    """TecDoc PDF 102-byte row: substring positions match postgresql_substring_example in CSV."""
    if len(rec) != T030_PDF_RECORD_LEN:
        logger.warning(
            "t030 PDF layout expected len=%s got=%s sample=%r",
            T030_PDF_RECORD_LEN,
            len(rec),
            rec[:80],
        )
        return None
    # 0-based slices for PostgreSQL 1-based FROM n FOR k (see tecdoc_column_string_positions.csv).
    parsed = {
        "reserviert": rec[0:22].strip(),
        "dlnr": rec[22:26].strip(),
        "sa": rec[26:29].strip(),
        "beznr": rec[29:38].strip(),
        "sprachnr": rec[38:41].strip(),
        "bez": rec[41:101].strip(),
        "lflag": rec[101:102].strip(),
    }
    out: list[str] = []
    for m in copy_columns:
        if m.name in parsed:
            out.append(parsed[m.name])
        else:
            out.append("")
    return out


def _line_to_copy_fields_t030(
    line: str,
    copy_columns: list[ColumnLoadMeta],
    *,
    layout: str,
) -> list[str] | None:
    """Parse 030 per ``layout`` (``supplier`` or ``pdf102``); see module docstring."""
    raw = line.rstrip("\r\n")
    if not raw.strip():
        return None

    if layout == "supplier":
        return _line_to_copy_fields_t030_supplier(raw.lstrip(), copy_columns)

    # pdf102
    if len(raw) == T030_PDF_RECORD_LEN:
        return _line_to_copy_fields_t030_pdf102(raw, copy_columns)
    return _line_to_copy_fields_t030_supplier(raw.lstrip(), copy_columns)


def _line_to_copy_fields_t209_48(
    rec: str, copy_columns: list[ColumnLoadMeta]
) -> list[str] | None:
    """
    Supplier 48-byte table 209: artnr, dlnr, sa, lkz, exclude, eannr (13), lflag, 1 filler byte.
    PK ``artnr_dlnr_lkz_eannr`` is computed (see ``_computed_pk_t209_raw``).
    """
    if len(rec) != 48:
        return None
    raw_by_name = {
        "artnr": rec[0:22],
        "dlnr": rec[22:26],
        "sa": rec[26:29],
        "lkz": rec[29:32],
        "exclude": rec[32:33],
        "eannr": rec[33:46],
        "lflag": rec[46:47],
    }
    parsed = {k: _trim_csv_slice_value(v) for k, v in raw_by_name.items()}
    out: list[str] = []
    for m in copy_columns:
        if m.name == "artnr_dlnr_lkz_eannr":
            try:
                out.append(_computed_pk_t209_raw(raw_by_name))
            except KeyError as e:
                logger.warning("t209 PK build missing field: %s", e)
                return None
            continue
        if m.name in parsed:
            out.append(parsed[m.name])
            continue
        out.append("")
    return out


def _line_to_copy_fields_t203_71(
    rec: str, copy_columns: list[ColumnLoadMeta]
) -> list[str] | None:
    """
    Supplier 71-byte table 203 (physical order differs from DDL column order).
    PK ``artnr_dlnr_khernr_refnr_sort`` = trimmed artnr + dlnr + khernr + refnr_raw + sort
    (no padding between parts).
    """
    if len(rec) != 71:
        return None
    raw_by_name = {
        "artnr": rec[0:22],
        "dlnr": rec[22:26],
        "sa": rec[26:29],
        "khernr": rec[29:35],
        "refnr_raw": rec[35:57],
        "referenzinfo": rec[57:60],
        "exclude": rec[60:61],
        "additiv": rec[61:62],
        "sort": rec[62:67],
        "lkz": rec[67:70],
        "lflag": rec[70:71],
    }
    parsed = {k: _trim_csv_slice_value(v) for k, v in raw_by_name.items()}
    out: list[str] = []
    for m in copy_columns:
        if m.name == "artnr_dlnr_khernr_refnr_sort":
            try:
                out.append(_computed_pk_t203_trimmed(parsed))
            except KeyError as e:
                logger.warning("t203 PK build missing field: %s", e)
                return None
            continue
        if m.name == "refnr":
            out.append(parsed["refnr_raw"])
            continue
        if m.name in parsed:
            out.append(parsed[m.name])
            continue
        out.append("")
    return out


def _line_to_copy_fields_t204_61(
    rec: str, copy_columns: list[ColumnLoadMeta]
) -> list[str] | None:
    """
    Supplier 61-byte table 204 when schema-width fallback would expect 135 (PDF layout).
    Same field map as ``documentation/tecdoc_column_string_positions.csv`` for t204.
    PK ``ersatznr_artnr_dlnr`` = raw ersatznr_raw + artnr + dlnr (padded slices).
    """
    if len(rec) != 61:
        return None
    raw_by_name = {
        "artnr": rec[0:22],
        "dlnr": rec[22:26],
        "sa": rec[26:29],
        "ersatznr_raw": rec[29:51],
        "sort": rec[51:56],
        "lflag": rec[56:61],
    }
    parsed = {k: _trim_csv_slice_value(v) for k, v in raw_by_name.items()}
    out: list[str] = []
    for m in copy_columns:
        if m.name == "ersatznr_artnr_dlnr":
            try:
                out.append(_computed_pk_t204_raw(raw_by_name))
            except KeyError as e:
                logger.warning("t204 PK build missing field: %s", e)
                return None
            continue
        if m.name == "ersatznr":
            out.append(parsed["ersatznr_raw"])
            continue
        if m.name in parsed:
            out.append(parsed[m.name])
            continue
        out.append("")
    return out


def _trim_csv_slice_value(raw: str) -> str:
    """
    Trim padding like ``trim(substring(...))``. If the slice is **only** whitespace, keep the raw
    slice so NOT NULL varchar columns (e.g. t042 ``lkz`` = three spaces) do not become empty/NULL.
    """
    s = raw.strip()
    if s:
        return s
    if raw and not raw.strip():
        return raw
    return ""


def line_to_copy_fields_csv_layout(
    line: str,
    table_name: str,
    copy_columns: list[ColumnLoadMeta],
    slices: dict[str, tuple[int, int]],
    min_line_len: int,
) -> list[str] | None:
    """
    Parse a line using ``pos_0based_start`` and ``field_length`` from
    ``documentation/tecdoc_column_string_positions.csv`` for each mapped column, then ``strip()``
    each value (equivalent to ``trim(substring(...))`` in the CSV’s PostgreSQL examples).
    ``min_line_len`` is the required minimum physical line length (PDF record length when known).

    **Do not** ``lstrip`` the physical record before calling: leading spaces are part of the line
    (e.g. table 030 ``reserviert`` / alignment). Only newline characters are stripped here.
    """
    line = line.rstrip("\r\n")
    if not line.strip():
        return None
    if len(line) < min_line_len:
        logger.warning(
            "Line too short for %s (CSV layout): len=%s need>=%s sample=%r",
            table_name,
            len(line),
            min_line_len,
            line[:120],
        )
        return None

    raw_by_name: dict[str, str] = {}
    for name, (start, ln) in slices.items():
        end = start + ln
        if end > len(line):
            logger.warning(
                "%s: field %s slice [%s:%s] exceeds line len=%s",
                table_name,
                name,
                start,
                end,
                len(line),
            )
            return None
        raw_by_name[name] = line[start:end]

    parsed = {k: _trim_csv_slice_value(v) for k, v in raw_by_name.items()}
    out: list[str] = []
    for m in copy_columns:
        if (
            table_name == "t400"
            and m.name == "artnr_dlnr_genartnr_vknzielart_vknzielnr_lfdnr"
        ):
            try:
                out.append(_computed_pk_t400_raw(raw_by_name))
            except KeyError as e:
                logger.warning("t400 PK build missing field: %s", e)
                return None
            continue
        if table_name == "t211" and m.name == "artnr_dlnr_genartnr":
            try:
                out.append(_computed_pk_t211_raw(raw_by_name))
            except KeyError as e:
                logger.warning("t211 PK build missing field: %s", e)
                return None
            continue
        if table_name == "t210" and m.name == "artnr_dlnr_sortnr":
            try:
                out.append(_computed_pk_t210_raw(raw_by_name))
            except (KeyError, ValueError) as e:
                logger.warning("t210 PK build failed: %s", e)
                return None
            continue
        if table_name == "t232" and m.name == "artnr_dlnr_sortnr":
            try:
                out.append(_computed_pk_t232_raw(raw_by_name))
            except (KeyError, ValueError) as e:
                logger.warning("t232 PK build failed: %s", e)
                return None
            continue
        if table_name == "t209" and m.name == "artnr_dlnr_lkz_eannr":
            try:
                out.append(_computed_pk_t209_raw(raw_by_name))
            except KeyError as e:
                logger.warning("t209 PK build missing field: %s", e)
                return None
            continue
        if table_name == "t203" and m.name == "artnr_dlnr_khernr_refnr_sort":
            try:
                out.append(_computed_pk_t203_trimmed(parsed))
            except KeyError as e:
                logger.warning("t203 PK build missing field: %s", e)
                return None
            continue
        if table_name == "t203" and m.name == "refnr":
            if "refnr_raw" not in parsed:
                logger.warning("t203: missing refnr_raw for refnr column")
                return None
            out.append(parsed["refnr_raw"])
            continue
        if table_name == "t204" and m.name == "ersatznr_artnr_dlnr":
            try:
                out.append(_computed_pk_t204_raw(raw_by_name))
            except KeyError as e:
                logger.warning("t204 PK build missing field: %s", e)
                return None
            continue
        if table_name == "t204" and m.name == "ersatznr":
            if "ersatznr_raw" not in parsed:
                logger.warning("t204: missing ersatznr_raw for ersatznr column")
                return None
            out.append(parsed["ersatznr_raw"])
            continue
        if m.name in parsed:
            out.append(parsed[m.name])
            continue
        out.append("")

    return out


def _fixed_width_for_column(m: ColumnLoadMeta, settings: Settings) -> int:
    """Return how many characters to read from the line for this column (see module doc)."""
    dt = m.data_type
    if m.udt_name == "tsvector":
        return 0
    if dt in ("character varying", "varchar", "character", "char", "bpchar"):
        if m.char_max_len is None:
            raise ValueError(
                f"Column {m.name}: character type without length (cannot derive fixed width)"
            )
        # DB column length n == number of characters to take from the string (varchar(n)).
        return int(m.char_max_len)
    if dt == "integer":
        return settings.fixed_width_integer_width
    if dt == "bigint":
        return settings.fixed_width_bigint_width
    if dt == "smallint":
        return settings.fixed_width_smallint_width
    if dt in ("numeric", "decimal"):
        if m.numeric_precision is not None:
            return int(m.numeric_precision)
        return 10
    raise ValueError(
        f"Column {m.name}: unsupported data type {dt!r} for fixed-width parsing"
    )


def _computed_pk_t400_raw(raw_by_name: dict[str, str]) -> str:
    """
    52-char PK = concatenation of fixed-width slices as in the flat file (padded),
    not stripped display values.
    Order: artnr, dlnr, genartnr, vknzielart, vknzielnr, lfdnr.
    """
    parts = (
        raw_by_name["artnr"],
        raw_by_name["dlnr"],
        raw_by_name["genartnr"],
        raw_by_name["vknzielart"],
        raw_by_name["vknzielnr"],
        raw_by_name["lfdnr"],
    )
    return "".join(parts)


def _computed_pk_t211_raw(raw_by_name: dict[str, str]) -> str:
    """31-char PK = artnr + dlnr + genartnr as fixed-width slices (padded), same as flat file."""
    return (
        raw_by_name["artnr"]
        + raw_by_name["dlnr"]
        + raw_by_name["genartnr"]
    )


def _computed_pk_t209_raw(raw_by_name: dict[str, str]) -> str:
    """42-char PK = artnr + dlnr + lkz + eannr (fixed-width slices, padded)."""
    return (
        raw_by_name["artnr"]
        + raw_by_name["dlnr"]
        + raw_by_name["lkz"]
        + raw_by_name["eannr"]
    )


def _computed_pk_t203_trimmed(parsed: dict[str, str]) -> str:
    """
    PK = trim(artnr) + trim(dlnr) + trim(khernr) + trim(refnr_raw) + trim(sort), no padding
    between parts (supplier 71-byte layout; see documentation/tecdoc_column_string_positions.csv).
    """
    return (
        parsed["artnr"]
        + parsed["dlnr"]
        + parsed["khernr"]
        + parsed["refnr_raw"]
        + parsed["sort"]
    )


def _computed_pk_t204_raw(raw_by_name: dict[str, str]) -> str:
    """48-char PK = ersatznr_raw + artnr + dlnr (fixed-width slices, padded)."""
    return (
        raw_by_name["ersatznr_raw"]
        + raw_by_name["artnr"]
        + raw_by_name["dlnr"]
    )


def _computed_pk_t210_raw(raw_by_name: dict[str, str]) -> str:
    """
    29-char PK = artnr + dlnr + first 3 characters of the 4-char ``kritnr`` field (supplier 67-byte
    layout). The trailing ``sortnr`` column is separate; multiple criteria rows share the same
    artnr/dlnr/sa prefix so the key must include the criterion discriminator.
    """
    kn = raw_by_name["kritnr"]
    if len(kn) < 3:
        raise ValueError("kritnr slice shorter than 3 characters")
    return raw_by_name["artnr"] + raw_by_name["dlnr"] + kn[:3]


def _computed_pk_t232_raw(raw_by_name: dict[str, str]) -> str:
    """
    29-char PK = artnr + dlnr + sortnr as fixed-width slices from the 47-byte line (same as t211),
    right-padded with spaces. The composite is not stored as a contiguous block in the flat file.
    """
    s = raw_by_name["artnr"] + raw_by_name["dlnr"] + raw_by_name["sortnr"]
    if len(s) > 29:
        raise ValueError("t232 PK longer than 29 characters")
    return s + (" " * (29 - len(s)))


def split_fixed_width_chunks(line: str, widths: list[int]) -> list[str]:
    """Slice line into chunks; each width is a character count (same order as columns)."""
    expected = sum(widths)
    if len(line) != expected:
        raise ValueError(f"Expected line length {expected}, got {len(line)}")
    out: list[str] = []
    pos = 0
    for w in widths:
        out.append(line[pos : pos + w])
        pos += w
    return out


def build_fixed_width_plan(
    table_name: str,
    all_meta: list[ColumnLoadMeta],
    settings: Settings,
) -> tuple[list[ColumnLoadMeta], list[ColumnLoadMeta], list[int]]:
    """
    Returns (copy_columns, parse_sequence, widths).
    copy_columns: DB order, no tsvector.
    parse_sequence: columns that appear consecutively in the raw line.
    widths: width for each parse_sequence column.
    """
    copy_columns = [m for m in all_meta if m.udt_name != "tsvector"]
    if not copy_columns:
        raise ValueError(f"{table_name}: no loadable columns")

    omit = DEFAULT_OMIT_FROM_RAW_LINE.get(table_name, frozenset()) | EXTRA_OMIT_FROM_RAW_LINE.get(
        table_name, frozenset()
    )
    parse_sequence = [m for m in copy_columns if m.name not in omit]
    widths = [_fixed_width_for_column(m, settings) for m in parse_sequence]
    if any(w <= 0 for w in widths):
        raise ValueError(f"{table_name}: invalid width in parse sequence")
    return copy_columns, parse_sequence, widths


def line_to_copy_fields_fixed(
    line: str,
    table_name: str,
    copy_columns: list[ColumnLoadMeta],
    parse_sequence: list[ColumnLoadMeta],
    widths: list[int],
    *,
    t030_layout: str = "supplier",
) -> list[str] | None:
    """
    Parse one fixed-width line into values aligned with copy_columns (same order as COPY).
    Returns None if the line length does not match (skip row).
    """
    line = line.rstrip("\r\n")
    if not line.strip():
        return None

    if table_name == "t030":
        return _line_to_copy_fields_t030(line, copy_columns, layout=t030_layout)

    if table_name == "t209" and len(line) == 48:
        return _line_to_copy_fields_t209_48(line, copy_columns)

    if table_name == "t203" and len(line) == 71:
        return _line_to_copy_fields_t203_71(line, copy_columns)

    if table_name == "t204" and len(line) == 61:
        return _line_to_copy_fields_t204_61(line, copy_columns)

    expected = sum(widths)
    if len(line) != expected:
        logger.warning(
            "Line length mismatch for %s: expected %s, got %s (sample %r)",
            table_name,
            expected,
            len(line),
            line[:120],
        )
        return None

    try:
        raw_chunks = split_fixed_width_chunks(line, widths)
    except ValueError:
        return None

    raw_by_name = {m.name: raw_chunks[i] for i, m in enumerate(parse_sequence)}
    # Match CSV path: whitespace-only slices stay as raw padding (e.g. t207 ``lkz`` = three spaces,
    # NOT NULL) instead of ``strip()`` → "" → NULL with EMPTY_AS_NULL.
    parsed = {name: _trim_csv_slice_value(chunk) for name, chunk in raw_by_name.items()}
    out: list[str] = []

    for m in copy_columns:
        if m.name in parsed:
            out.append(parsed[m.name])
            continue
        if (
            table_name == "t400"
            and m.name == "artnr_dlnr_genartnr_vknzielart_vknzielnr_lfdnr"
        ):
            try:
                out.append(_computed_pk_t400_raw(raw_by_name))
            except KeyError as e:
                logger.warning("t400 PK build missing field: %s", e)
                return None
            continue
        if table_name == "t211" and m.name == "artnr_dlnr_genartnr":
            try:
                out.append(_computed_pk_t211_raw(raw_by_name))
            except KeyError as e:
                logger.warning("t211 PK build missing field: %s", e)
                return None
            continue
        if table_name == "t210" and m.name == "artnr_dlnr_sortnr":
            try:
                out.append(_computed_pk_t210_raw(raw_by_name))
            except (KeyError, ValueError) as e:
                logger.warning("t210 PK build failed: %s", e)
                return None
            continue
        if table_name == "t232" and m.name == "artnr_dlnr_sortnr":
            try:
                out.append(_computed_pk_t232_raw(raw_by_name))
            except (KeyError, ValueError) as e:
                logger.warning("t232 PK build failed: %s", e)
                return None
            continue
        if table_name == "t209" and m.name == "artnr_dlnr_lkz_eannr":
            try:
                out.append(_computed_pk_t209_raw(raw_by_name))
            except KeyError as e:
                logger.warning("t209 PK build missing field: %s", e)
                return None
            continue
        if table_name == "t203" and m.name == "artnr_dlnr_khernr_refnr_sort":
            try:
                out.append(_computed_pk_t203_trimmed(parsed))
            except KeyError as e:
                logger.warning("t203 PK build missing field: %s", e)
                return None
            continue
        if table_name == "t203" and m.name == "refnr":
            if "ersatznr_raw" not in parsed:
                logger.warning("t203: missing ersatznr_raw for refnr column")
                return None
            out.append(parsed["ersatznr_raw"])
            continue
        if table_name == "t204" and m.name == "ersatznr_artnr_dlnr":
            try:
                out.append(_computed_pk_t204_raw(raw_by_name))
            except KeyError as e:
                logger.warning("t204 PK build missing field: %s", e)
                return None
            continue
        if table_name == "t204" and m.name == "ersatznr":
            if "ersatznr_raw" not in parsed:
                logger.warning("t204: missing ersatznr_raw for ersatznr column")
                return None
            out.append(parsed["ersatznr_raw"])
            continue
        # Omitted from raw line (e.g. t200 artnr_raw) → empty → NULL if EMPTY_AS_NULL
        out.append("")

    return out
