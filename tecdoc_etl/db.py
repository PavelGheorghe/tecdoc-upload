import logging
from typing import Any

import psycopg2
import psycopg2.extensions
from psycopg2 import sql
from psycopg2 import errors as pg_errors

from tecdoc_etl.config import ETL_SCHEMA, SCHEMA, Settings

logger = logging.getLogger(__name__)


def connect(settings: Settings) -> psycopg2.extensions.connection:
    if not settings.database_url:
        raise ValueError("DATABASE_URL is not set")
    conn = psycopg2.connect(settings.database_url)
    conn.autocommit = False
    return conn


def ensure_etl_schema(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(ETL_SCHEMA))
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {ETL_SCHEMA}.sync_jobs (
                id SERIAL PRIMARY KEY,
                started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                finished_at TIMESTAMPTZ,
                status TEXT NOT NULL,
                dry_run BOOLEAN NOT NULL DEFAULT false,
                message TEXT,
                progress JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                errors JSONB NOT NULL DEFAULT '[]'::jsonb
            )
            """
        )
    conn.commit()


def list_schema_tables(conn: psycopg2.extensions.connection) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT tablename FROM pg_tables
            WHERE schemaname = %s
            ORDER BY tablename
            """,
            (SCHEMA,),
        )
        return [r[0] for r in cur.fetchall()]


def get_table_columns(conn: psycopg2.extensions.connection, table: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (SCHEMA, table),
        )
        return [r[0] for r in cur.fetchall()]


class ColumnLoadMeta:
    """Per-column metadata from PostgreSQL ``information_schema.columns``."""

    __slots__ = (
        "name",
        "data_type",
        "udt_name",
        "char_max_len",
        "numeric_precision",
        "numeric_scale",
    )

    def __init__(
        self,
        name: str,
        data_type: str,
        udt_name: str,
        char_max_len: int | None,
        numeric_precision: int | None,
        numeric_scale: int | None,
    ) -> None:
        self.name = name
        self.data_type = data_type
        self.udt_name = udt_name
        # character_maximum_length for varchar(n): n characters to take from each flat line.
        self.char_max_len = char_max_len
        self.numeric_precision = numeric_precision
        self.numeric_scale = numeric_scale


def get_table_column_load_meta(
    conn: psycopg2.extensions.connection, table: str
) -> list[ColumnLoadMeta]:
    """Column metadata for building fixed-width layouts (excludes tsvector in loader)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, udt_name, character_maximum_length,
                   numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (SCHEMA, table),
        )
        rows = cur.fetchall()
    return [
        ColumnLoadMeta(
            name=r[0],
            data_type=(r[1] or "").lower(),
            udt_name=(r[2] or "").lower(),
            char_max_len=r[3],
            numeric_precision=r[4],
            numeric_scale=r[5],
        )
        for r in rows
    ]


def list_tables_with_dlnr_column(
    conn: psycopg2.extensions.connection,
) -> list[tuple[str, str]]:
    """
    Return (table_name, data_type) for base tables in SCHEMA that have a column ``dlnr``.
    ``data_type`` is from information_schema (e.g. character varying, numeric).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.table_name, c.data_type
            FROM information_schema.columns c
            JOIN information_schema.tables t
              ON t.table_schema = c.table_schema AND t.table_name = c.table_name
            WHERE c.table_schema = %s
              AND c.column_name = 'dlnr'
              AND t.table_type = 'BASE TABLE'
            ORDER BY c.table_name
            """,
            (SCHEMA,),
        )
        return [(r[0], (r[1] or "").lower()) for r in cur.fetchall()]


def get_primary_key_columns(
    conn: psycopg2.extensions.connection,
    table_name: str,
) -> list[str]:
    """Return PK column names in ordinal order for a table in SCHEMA, or [] if no PK."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON kcu.constraint_name = tc.constraint_name
             AND kcu.table_schema    = tc.table_schema
             AND kcu.table_name      = tc.table_name
            WHERE tc.table_schema    = %s
              AND tc.table_name      = %s
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            """,
            (SCHEMA, table_name),
        )
        return [r[0] for r in cur.fetchall()]


def normalize_supplier_folder_to_dlnr_match(folder_name: str) -> tuple[str, int | None]:
    """
    Map a D_TAF subdirectory name to values for SQL deletes.

    Returns (varchar_compare, int_value_or_none). For all-digit names, varchar_compare is
    zero-padded to 4 characters and int_value is the integer (for numeric ``dlnr`` columns).
    """
    name = folder_name.strip()
    if name.isdigit():
        n = int(name)
        return (name.zfill(4), n)
    return (name, None)


def delete_tecdoc_rows_for_dlnr(
    conn: psycopg2.extensions.connection,
    folder_name: str,
    *,
    log: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Remove rows for one data supplier from all ``tecdoc_gheorghe`` tables that expose ``dlnr``.

    Repeats deletes in a loop to satisfy foreign keys (children before parents). If a pass
    makes no progress, raises RuntimeError.
    """
    lg = log or logger
    vmatch, num_val = normalize_supplier_folder_to_dlnr_match(folder_name)
    tables = list_tables_with_dlnr_column(conn)
    if not tables:
        return {"tables_touched": 0, "rows_deleted_total": 0, "detail": []}

    remaining = list(tables)
    total_deleted = 0
    detail: list[dict[str, Any]] = []
    max_passes = 25
    for pass_no in range(max_passes):
        if not remaining:
            break
        progressed = False
        still: list[tuple[str, str]] = []
        for table, dtype in remaining:
            qual = sql.SQL("{}.{}").format(sql.Identifier(SCHEMA), sql.Identifier(table))
            try:
                with conn.cursor() as cur:
                    if "char" in dtype or dtype == "text":
                        cur.execute(
                            sql.SQL("DELETE FROM {} WHERE btrim(dlnr::text) = btrim(%s)").format(
                                qual
                            ),
                            (vmatch,),
                        )
                    elif dtype in ("smallint", "integer", "bigint") or "numeric" in dtype:
                        if num_val is None:
                            lg.warning(
                                "Skipping %s.%s: numeric dlnr and non-numeric folder %r",
                                SCHEMA,
                                table,
                                folder_name,
                            )
                            still.append((table, dtype))
                            continue
                        cur.execute(
                            sql.SQL("DELETE FROM {} WHERE dlnr = %s").format(qual),
                            (num_val,),
                        )
                    else:
                        cur.execute(
                            sql.SQL("DELETE FROM {} WHERE dlnr::text = %s").format(qual),
                            (vmatch,),
                        )
                    n = cur.rowcount
                conn.commit()
                progressed = True
                if n:
                    total_deleted += n
                    detail.append({"table": table, "rows": n, "pass": pass_no + 1})
                    lg.info(
                        "Deleted %s row(s) from %s.%s for supplier %r",
                        n,
                        SCHEMA,
                        table,
                        folder_name,
                    )
            except pg_errors.ForeignKeyViolation:
                conn.rollback()
                still.append((table, dtype))
            except Exception:
                conn.rollback()
                raise
        remaining = still
        if not progressed and remaining:
            names = [t for t, _ in remaining]
            raise RuntimeError(
                f"Could not delete rows for supplier {folder_name!r}: "
                f"foreign keys block remaining tables {names}"
            )
    if remaining:
        raise RuntimeError(
            f"delete_tecdoc_rows_for_dlnr: exceeded {max_passes} passes; left {remaining!r}"
        )
    return {
        "tables_touched": len(detail),
        "rows_deleted_total": total_deleted,
        "detail": detail,
    }


def truncate_all_tecdoc_tables(conn: psycopg2.extensions.connection) -> None:
    tables = list_schema_tables(conn)
    if not tables:
        return
    with conn.cursor() as cur:
        parts = [
            sql.SQL("{}.{}").format(sql.Identifier(SCHEMA), sql.Identifier(t))
            for t in tables
        ]
        stmt = sql.SQL("TRUNCATE TABLE {} CASCADE").format(sql.SQL(", ").join(parts))
        cur.execute(stmt)
    conn.commit()


def ping_db(conn: psycopg2.extensions.connection) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        return cur.fetchone()[0] == 1
