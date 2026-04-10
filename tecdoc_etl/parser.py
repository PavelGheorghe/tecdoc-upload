"""
Delimited-row parsing (only when ``ROW_PARSE_MODE=delimiter``).

For the default ``fixed_width`` mode, field sizes come from DB column lengths; see
``tecdoc_etl.line_layout``.
"""

import csv
import io


def parse_delimited_row(line: str, delimiter: str) -> list[str]:
    """Parse one logical row without header; supports RFC4180-style quoting."""
    line = line.rstrip("\r\n")
    if not line:
        return []
    reader = csv.reader(
        io.StringIO(line),
        delimiter=delimiter,
        quoting=csv.QUOTE_MINIMAL,
        doublequote=True,
        strict=False,
    )
    try:
        row = next(reader)
    except StopIteration:
        return []
    return [str(c) if c is not None else "" for c in row]


def escape_copy_text_field(value: str | None, empty_as_null: bool) -> str:
    if value is None:
        return "\\N"
    if empty_as_null and value == "":
        return "\\N"
    s = str(value)
    return (
        s.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def row_to_copy_line(
    fields: list[str], col_count: int, empty_as_null: bool
) -> str:
    while len(fields) < col_count:
        fields.append("")
    fields = fields[:col_count]
    return "\t".join(escape_copy_text_field(f, empty_as_null) for f in fields) + "\n"
