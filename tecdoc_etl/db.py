import psycopg2
import psycopg2.extensions
from psycopg2 import sql

from tecdoc_etl.config import ETL_SCHEMA, SCHEMA, Settings


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
