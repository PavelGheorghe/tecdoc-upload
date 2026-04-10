#!/usr/bin/env python3
"""
Build documentation/tecdoc_column_string_positions.csv from:
  - documentation/TecDoc-Data-Format_Version_2.7_EN_2.0.31.pdf (field Pos/Length)
  - documentation/tecdoc_structure.sql-compatible DDL (column names & types)

Positions are TecDoc character indices (0-based in PDF "Pos" column).
PostgreSQL substring uses 1-based FROM; inclusive TO = FROM + length - 1.

Run from repo root: python scripts/generate_tecdoc_column_map_csv.py
Requires: pypdf
"""

from __future__ import annotations

import csv
import re
import unicodedata
from pathlib import Path

try:
    from pypdf import PdfReader
except ImportError as e:
    raise SystemExit("Install pypdf: pip install pypdf") from e

REPO = Path(__file__).resolve().parents[1]
PDF_PATH = REPO / "documentation" / "TecDoc-Data-Format_Version_2.7_EN_2.0.31.pdf"
DDL_PATH = REPO / "documentation" / "tecdoc_structure.txt"
OUT_CSV = REPO / "documentation" / "tecdoc_column_string_positions.csv"
SCHEMA = "tecdoc_gheorghe"

# Skip TOC lines (many dots)
RE_TOC = re.compile(r"\.{8,}")

# Table title: "030 Language Descriptions" (not "Page 44")
RE_TABLE_HEAD = re.compile(r"^(\d{3})\s+([A-Za-z0-9].+)$")
RE_LENGTH = re.compile(r"^Length:\s*(\d+)")
# Field row ends with Pos (3 digits) Len Type Description
RE_FIELD_TAIL = re.compile(
    r"^(.+?)\s+(\d{3})\s+(\d+)\s+(\([NCUL]\)|[NCUL])\s*(.*)$"
)


def fold_key(s: str) -> str:
    """Alphanumeric fold for matching TecDoc old names to SQL identifiers."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if c.isalnum())
    return s.lower()


def extract_pdf_text() -> str:
    r = PdfReader(str(PDF_PATH))
    parts: list[str] = []
    for p in r.pages:
        parts.append(p.extract_text() or "")
    return "\n".join(parts)


def parse_ddl_columns(path: Path) -> dict[str, list[tuple[str, str]]]:
    """
    table_name -> [(col_name, type_spec), ...] in DDL order.
    type_spec is the raw fragment e.g. varchar(9), integer, tsvector.
    """
    text = path.read_text(encoding="utf-8", errors="replace")
    tables: dict[str, list[tuple[str, str]]] = {}
    current: str | None = None
    in_create = False
    for raw in text.splitlines():
        line = raw.strip()
        m = re.match(
            r"create table if not exists\s+" + re.escape(SCHEMA) + r"\.(t\d+)\s*(?:\(|$)",
            line,
            re.I,
        )
        if m:
            current = m.group(1).lower()
            tables[current] = []
            in_create = True
            continue
        if in_create and line.startswith(")"):
            in_create = False
            current = None
            continue
        if not in_create or not current or not line or line.startswith("--"):
            continue
        # constraint / index lines inside create are not in this DDL file between parens
        if line.lower().startswith("constraint ") or line.lower().startswith("primary key"):
            continue
        if line.endswith("(") and "create" not in line.lower():
            continue
        low = line.lower()
        type_start = None
        for kw in (
            "varchar",
            "character varying",
            "integer",
            "bigint",
            "smallint",
            "numeric",
            "tsvector",
            "double precision",
        ):
            pos = low.find(kw)
            if pos > 0 and (type_start is None or pos < type_start):
                type_start = pos
        if type_start is None:
            continue
        col_part = line[:type_start].strip()
        if col_part.startswith('"') and col_part.endswith('"'):
            col = col_part[1:-1]
        else:
            col = col_part.split()[0] if col_part.split() else col_part
        rest = line[type_start:].rstrip(",").strip()
        if rest.lower().startswith("constraint"):
            continue
        tables[current].append((col, rest))
    return tables


def is_toc_line(line: str) -> bool:
    return bool(RE_TOC.search(line))


def parse_tecdoc_tables(pdf_text: str) -> dict[str, dict]:
    """
    table_num '001'..'999' -> {title, record_length, fields: [{pos,len,type,name_prefix,raw_line},...]}
    """
    lines = pdf_text.splitlines()
    out: dict[str, dict] = {}
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        m = RE_TABLE_HEAD.match(line)
        if not m or is_toc_line(line) or line.lower().startswith("page "):
            i += 1
            continue
        tbl, title = m.group(1), m.group(2).strip()
        if tbl not in out:
            # Look ahead for Length
            rec_len = None
            j = i + 1
            while j < min(i + 40, len(lines)) and j < len(lines):
                lm = RE_LENGTH.match(lines[j].strip())
                if lm:
                    rec_len = int(lm.group(1))
                    break
                j += 1
            if rec_len is None:
                i += 1
                continue
            # Find header row with Pos
            k = j + 1
            header_at = None
            while k < min(j + 25, len(lines)):
                if "Pos" in lines[k] and "Length" in lines[k]:
                    header_at = k
                    break
                k += 1
            if header_at is None:
                i += 1
                continue
            fields: list[dict] = []
            k = header_at + 1
            while k < len(lines):
                raw = lines[k].rstrip()
                if raw.strip().startswith("---PAGE"):
                    break
                if RE_TABLE_HEAD.match(raw.strip()) and not is_toc_line(raw):
                    nm = RE_TABLE_HEAD.match(raw.strip())
                    if nm and nm.group(1) != tbl:
                        break
                if RE_LENGTH.match(raw.strip()) and fields:
                    break
                if raw.strip().lower().startswith("annotation:"):
                    k += 1
                    continue
                if raw.strip().lower().startswith("comment:"):
                    k += 1
                    continue
                tm = RE_FIELD_TAIL.match(raw.strip())
                if tm:
                    prefix, pos_s, flen_s, typ, desc = tm.groups()
                    pos = int(pos_s)
                    flen = int(flen_s)
                    fields.append(
                        {
                            "pos": pos,
                            "length": flen,
                            "type": typ.strip("()"),
                            "name_prefix": prefix.strip(),
                            "description": (desc or "").strip(),
                            "raw_line": raw.strip()[:500],
                        }
                    )
                elif fields and raw.strip() and not raw.strip().startswith("---"):
                    # continuation of description
                    fields[-1]["description"] = (
                        fields[-1].get("description", "") + " " + raw.strip()
                    ).strip()
                k += 1
            out[tbl] = {
                "title": title,
                "record_length": rec_len,
                "fields": fields,
            }
        i += 1
    return out


def split_name_old(prefix: str) -> tuple[str, str]:
    """Split 'Reserved  Reserviert' style into (english_name, old_name)."""
    parts = re.split(r"\s{2,}", prefix.strip())
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    return prefix.strip(), ""


def match_db_column(
    table: str,
    eng: str,
    old: str,
    ddl_cols: list[tuple[str, str]],
) -> tuple[str | None, str]:
    """Return (db_column_name, note)."""
    cols = [c for c, t in ddl_cols if "tsvector" not in t.lower()]
    keys_old = fold_key(old)
    keys_eng = fold_key(eng)
    key_aliases = {
        "reserved": "reserviert",
        "reserviert": "reserviert",
        "brandno": "dlnr",
        "dlnr": "dlnr",
        "tableno": "sa",
        "langno": "sprachnr",
        "termno": "beznr",
        "ctermno": "lbeznr",
        "curno": "warnr",
        "curcode": "wkz",
        "curtermno": "warbeznr",
        "areacode": "vorwahl",
        "isgroup": "lstgruppe",
        "countrycode": "lkz",
        "doctype": "dokumentenart",
        "docno": "dokumentenart",
        "critno": "kritnr",
        "tabno": "tabnr",
        "manno": "hernr",
        "brandname": "marke",
        "refdataversion": "referenzdat",
        "fulldeliverykz": "kzvoll",
        "mandatarelease": "release",
        "versiondate": "datum",
        "datarelease": "release",
        "fullkzvoll": "kzvoll",
        "manno": "khernr",
        "hernr": "khernr",
        "referenzdaten": "referenzdat",
    }
    for col, _ in ddl_cols:
        ck = fold_key(col)
        if keys_old and ck == keys_old:
            return col, "match_old_name"
        if keys_eng and ck == keys_eng:
            return col, "match_tecdoc_name"
    for src, tgt in key_aliases.items():
        if keys_old == src or keys_eng == src:
            for col, _ in ddl_cols:
                if fold_key(col) == tgt:
                    return col, "match_alias"
    # "Term Bez" / description text field (PDF merges name)
    if keys_eng in ("termbez", "term") or (keys_eng.startswith("term") and "bez" in keys_eng):
        for col, _ in ddl_cols:
            if fold_key(col) == "bez":
                return col, "heuristic_term_bez"
    # Heuristic: Losch / delete flag
    if "losch" in keys_old or keys_old.startswith("deleteflag"):
        for col, _ in ddl_cols:
            if col in ("lflag", "losch_flag"):
                return col, "heuristic_delete_flag"
    if keys_old == "exclude" or keys_eng == "exclude":
        for col, _ in ddl_cols:
            if col == "exclude":
                return col, "match_exclude"
    return None, "unmatched"


def main() -> None:
    ddl = parse_ddl_columns(DDL_PATH)
    pdf_text = extract_pdf_text()
    tecdoc = parse_tecdoc_tables(pdf_text)

    rows: list[dict[str, str]] = []

    for tbl_name in sorted(ddl.keys()):
        num = tbl_name[1:].lstrip("0") or "0"
        num3 = f"{int(num):03d}" if num.isdigit() else tbl_name[1:]
        spec = tecdoc.get(num3)

        if not spec or not spec["fields"]:
            for ord_i, (col, typ) in enumerate(ddl[tbl_name], start=1):
                if "tsvector" in typ.lower():
                    continue
                rows.append(
                    {
                        "schema_name": SCHEMA,
                        "table_name": tbl_name,
                        "db_column_ordinal": str(ord_i),
                        "db_column_name": col,
                        "pg_type": typ.split()[0] if typ else "",
                        "pdf_table_no": num3,
                        "pdf_table_title": "",
                        "pdf_record_length": "",
                        "tecdoc_physical_order": "",
                        "pos_0based_start": "",
                        "pos_0based_end_exclusive": "",
                        "pos_0based_range": "",
                        "pos_1based_from": "",
                        "pos_1based_to_inclusive": "",
                        "pos_1based_range_inclusive": "",
                        "field_length": "",
                        "tecdoc_field_type": "",
                        "tecdoc_name": "",
                        "tecdoc_old_name": "",
                        "postgresql_substring_example": "",
                        "mapping_note": "no_pdf_layout_extracted_for_table",
                    }
                )
            continue

        rec_len = spec["record_length"]
        title = spec["title"]
        # Map db column -> first matching PDF field (physical order + slice)
        col_to_field: dict[str, tuple[int, dict]] = {}
        for phys_i, f in enumerate(spec["fields"], start=1):
            eng, old = split_name_old(f["name_prefix"])
            db_col, _how = match_db_column(tbl_name, eng, old, ddl[tbl_name])
            if db_col and db_col not in col_to_field:
                col_to_field[db_col] = (phys_i, f)

        for ord_i, (col, typ) in enumerate(ddl[tbl_name], start=1):
            if "tsvector" in typ.lower():
                continue
            pg_t = typ.split()[0] if typ else ""
            hit = col_to_field.get(col)
            if not hit:
                rows.append(
                    {
                        "schema_name": SCHEMA,
                        "table_name": tbl_name,
                        "db_column_ordinal": str(ord_i),
                        "db_column_name": col,
                        "pg_type": pg_t,
                        "pdf_table_no": num3,
                        "pdf_table_title": title[:200],
                        "pdf_record_length": str(rec_len),
                        "tecdoc_physical_order": "",
                        "pos_0based_start": "",
                        "pos_0based_end_exclusive": "",
                        "pos_0based_range": "",
                        "pos_1based_from": "",
                        "pos_1based_to_inclusive": "",
                        "pos_1based_range_inclusive": "",
                        "field_length": "",
                        "tecdoc_field_type": "",
                        "tecdoc_name": "",
                        "tecdoc_old_name": "",
                        "postgresql_substring_example": "",
                        "mapping_note": "ddl_column_not_in_standard_pdf_field_list",
                    }
                )
                continue
            phys_i, f = hit
            pos = f["pos"]
            ln = f["length"]
            p0_ex = pos + ln
            p1 = pos + 1
            p1_to = pos + ln
            eng, old = split_name_old(f["name_prefix"])
            sub = ""
            if ln > 0:
                sub = f"trim(substring($1::text FROM {p1} FOR {ln}))"
            rows.append(
                {
                    "schema_name": SCHEMA,
                    "table_name": tbl_name,
                    "db_column_ordinal": str(ord_i),
                    "db_column_name": col,
                    "pg_type": pg_t,
                    "pdf_table_no": num3,
                    "pdf_table_title": title[:200],
                    "pdf_record_length": str(rec_len),
                    "tecdoc_physical_order": str(phys_i),
                    "pos_0based_start": str(pos),
                    "pos_0based_end_exclusive": str(p0_ex),
                    "pos_0based_range": f"{pos}-{p0_ex - 1}",
                    "pos_1based_from": str(p1),
                    "pos_1based_to_inclusive": str(p1_to),
                    "pos_1based_range_inclusive": f"{p1}-{p1_to}",
                    "field_length": str(ln),
                    "tecdoc_field_type": f["type"],
                    "tecdoc_name": eng,
                    "tecdoc_old_name": old,
                    "postgresql_substring_example": sub,
                    "mapping_note": "matched_pdf_field",
                }
            )

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "schema_name",
        "table_name",
        "db_column_ordinal",
        "db_column_name",
        "pg_type",
        "pdf_table_no",
        "pdf_table_title",
        "pdf_record_length",
        "tecdoc_physical_order",
        "pos_0based_start",
        "pos_0based_end_exclusive",
        "pos_0based_range",
        "pos_1based_from",
        "pos_1based_to_inclusive",
        "pos_1based_range_inclusive",
        "field_length",
        "tecdoc_field_type",
        "tecdoc_name",
        "tecdoc_old_name",
        "postgresql_substring_example",
        "mapping_note",
    ]
    with OUT_CSV.open("w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    print(f"Wrote {len(rows)} rows to {OUT_CSV}")


if __name__ == "__main__":
    main()
