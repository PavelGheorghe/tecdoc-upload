import json
from typing import Any

import psycopg2.extensions
from psycopg2.extras import Json

from tecdoc_etl.config import ETL_SCHEMA


def create_job(
    conn: psycopg2.extensions.connection, *, dry_run: bool
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {ETL_SCHEMA}.sync_jobs (status, dry_run)
            VALUES (%s, %s)
            RETURNING id
            """,
            ("running", dry_run),
        )
        jid = cur.fetchone()[0]
    conn.commit()
    return jid


def update_job(
    conn: psycopg2.extensions.connection,
    job_id: int,
    *,
    status: str | None = None,
    message: str | None = None,
    progress: dict[str, Any] | None = None,
    errors: list[dict[str, Any]] | None = None,
    finished: bool = False,
) -> None:
    parts: list[str] = []
    vals: list[Any] = []
    if status:
        parts.append("status = %s")
        vals.append(status)
    if message is not None:
        parts.append("message = %s")
        vals.append(message)
    if progress is not None:
        parts.append("progress = %s")
        vals.append(Json(progress))
    if errors is not None:
        parts.append("errors = %s")
        vals.append(Json(errors))
    if finished:
        parts.append("finished_at = now()")
    if not parts:
        return
    vals.append(job_id)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {ETL_SCHEMA}.sync_jobs
            SET {", ".join(parts)}
            WHERE id = %s
            """,
            vals,
        )
    conn.commit()


def append_error(
    conn: psycopg2.extensions.connection,
    job_id: int,
    err: dict[str, Any],
    *,
    current_errors: list[Any],
) -> None:
    current_errors.append(err)
    update_job(conn, job_id, errors=current_errors)


def get_job(conn: psycopg2.extensions.connection, job_id: int) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, started_at, finished_at, status, dry_run, message, progress, errors
            FROM {ETL_SCHEMA}.sync_jobs WHERE id = %s
            """,
            (job_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "started_at": row[1].isoformat() if row[1] else None,
        "finished_at": row[2].isoformat() if row[2] else None,
        "status": row[3],
        "dry_run": row[4],
        "message": row[5],
        "progress": row[6] if isinstance(row[6], dict) else json.loads(row[6] or "{}"),
        "errors": row[7] if isinstance(row[7], list) else json.loads(row[7] or "[]"),
    }


def list_jobs(conn: psycopg2.extensions.connection, limit: int = 50) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, started_at, finished_at, status, dry_run, message, progress, errors
            FROM {ETL_SCHEMA}.sync_jobs
            ORDER BY id DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    out = []
    for row in rows:
        out.append(
            {
                "id": row[0],
                "started_at": row[1].isoformat() if row[1] else None,
                "finished_at": row[2].isoformat() if row[2] else None,
                "status": row[3],
                "dry_run": row[4],
                "message": row[5],
                "progress": row[6]
                if isinstance(row[6], dict)
                else json.loads(row[6] or "{}"),
                "errors": row[7]
                if isinstance(row[7], list)
                else json.loads(row[7] or "[]"),
            }
        )
    return out
