#!/usr/bin/env python3
"""
Apply documentation/tecdoc_structure.txt to the database in DATABASE_URL.
Splits on statement boundaries (semicolon + newline). Skips empty lines and -- comments.
"""
import os
import re
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


def normalize_url(url: str) -> str:
    if url.startswith("pgsql://"):
        return "postgresql://" + url[len("pgsql://") :]
    return url


def strip_comments(sql: str) -> str:
    out = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        out.append(line)
    return "\n".join(out)


def split_statements(sql: str) -> list[str]:
    text = strip_comments(sql)
    # Split on semicolon followed by whitespace and newline (end of statement)
    parts = re.split(r";\s*\n", text)
    stmts = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if not p.endswith(";"):
            p += ";"
        stmts.append(p)
    return stmts


def main() -> int:
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("DATABASE_URL is not set", file=sys.stderr)
        return 1
    url = normalize_url(url)
    schema_path = ROOT / "documentation" / "tecdoc_structure.txt"
    if not schema_path.is_file():
        print(f"Missing {schema_path}", file=sys.stderr)
        return 1
    sql_text = schema_path.read_text(encoding="utf-8")
    statements = split_statements(sql_text)
    conn = psycopg2.connect(url)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for i, stmt in enumerate(statements):
                try:
                    cur.execute(stmt)
                except Exception as e:
                    print(f"Error in statement {i + 1}/{len(statements)}:\n{stmt[:200]}…\n{e}", file=sys.stderr)
                    return 1
        print(f"Applied {len(statements)} statements from {schema_path}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
