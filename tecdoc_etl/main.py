import logging
from pathlib import Path

from fastapi import BackgroundTasks, Body, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from tecdoc_etl.config import load_settings, mask_database_url
from tecdoc_etl.db import connect, ensure_etl_schema, ping_db
from tecdoc_etl.jobs import create_job, get_job, list_jobs
from tecdoc_etl.sync import (
    list_data_folders,
    list_local_archives,
    resolve_data_folder_basename,
    resolve_local_archive_basename,
    run_folder_load_task,
    run_local_archive_task,
    run_sync_task,
    sync_busy,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

settings = load_settings()
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app = FastAPI(title="TecDoc ETL", version="1.0.0")


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "title": "TecDoc ETL",
        },
    )


@app.get("/api/health")
def api_health() -> dict:
    if not settings.database_url:
        return {
            "database": "not_configured",
            "database_url_masked": None,
        }
    conn = connect(settings)
    try:
        ensure_etl_schema(conn)
        ok = ping_db(conn)
        return {
            "database": "ok" if ok else "error",
            "database_url_masked": mask_database_url(settings.database_url),
        }
    finally:
        conn.close()


@app.post("/api/jobs/sync")
def api_start_sync(
    background_tasks: BackgroundTasks,
    body: dict | None = Body(default=None),
) -> dict:
    payload = body or {}
    dry_run = bool(payload.get("dry_run"))
    if sync_busy():
        raise HTTPException(status_code=409, detail="A sync job is already running.")
    conn = connect(settings)
    try:
        ensure_etl_schema(conn)
        jid = create_job(conn, dry_run=dry_run)
    finally:
        conn.close()

    def work() -> None:
        try:
            run_sync_task(settings, job_id=jid, dry_run=dry_run)
        except Exception:
            logger.exception("Background sync failed")

    background_tasks.add_task(work)
    return {"job_id": jid, "status": "started"}


@app.get("/api/jobs")
def api_list_jobs(limit: int = 50) -> dict:
    conn = connect(settings)
    try:
        ensure_etl_schema(conn)
        jobs = list_jobs(conn, limit=limit)
    finally:
        conn.close()
    return {"jobs": jobs}


@app.get("/api/jobs/{job_id}")
def api_get_job(job_id: int) -> dict:
    conn = connect(settings)
    try:
        ensure_etl_schema(conn)
        job = get_job(conn, job_id)
    finally:
        conn.close()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.get("/api/status/sync")
def api_sync_status() -> dict:
    return {"busy": sync_busy()}


@app.get("/api/local-archives")
def api_local_archives() -> dict:
    """List *.7z basenames in DATA_DIR (non-recursive)."""
    return {"archives": list_local_archives(settings)}


@app.post("/api/jobs/load-local")
def api_load_local_archive(
    background_tasks: BackgroundTasks,
    body: dict | None = Body(default=None),
) -> dict:
    payload = body or {}
    filename = payload.get("filename")
    if not filename or not isinstance(filename, str):
        raise HTTPException(
            status_code=400, detail='Body must include string "filename" (e.g. 0001.7z).'
        )
    if sync_busy():
        raise HTTPException(status_code=409, detail="A sync job is already running.")
    try:
        archive_path = resolve_local_archive_basename(settings, filename)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except FileNotFoundError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    conn = connect(settings)
    try:
        ensure_etl_schema(conn)
        jid = create_job(conn, dry_run=False)
    finally:
        conn.close()

    def work() -> None:
        try:
            run_local_archive_task(settings, job_id=jid, archive_path=archive_path)
        except Exception:
            logger.exception("Background local archive load failed")

    background_tasks.add_task(work)
    return {"job_id": jid, "status": "started", "filename": archive_path.name}


@app.get("/supplier", response_class=HTMLResponse)
async def supplier_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "supplier.html",
        {
            "request": request,
            "title": "Local supplier archive",
        },
    )


@app.get("/api/data-folders")
def api_data_folders() -> dict:
    """Immediate subdirectories of DATA_DIR (e.g. 0001 for data/0001)."""
    return {"folders": list_data_folders(settings)}


@app.post("/api/jobs/load-folder")
def api_load_folder(
    background_tasks: BackgroundTasks,
    body: dict | None = Body(default=None),
) -> dict:
    payload = body or {}
    folder = payload.get("folder")
    if not folder or not isinstance(folder, str):
        raise HTTPException(
            status_code=400,
            detail='Body must include string "folder" (e.g. "0001" for data/0001).',
        )
    if sync_busy():
        raise HTTPException(status_code=409, detail="A sync job is already running.")
    try:
        folder_path = resolve_data_folder_basename(settings, folder)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except FileNotFoundError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    conn = connect(settings)
    try:
        ensure_etl_schema(conn)
        jid = create_job(conn, dry_run=False)
    finally:
        conn.close()

    def work() -> None:
        try:
            run_folder_load_task(settings, job_id=jid, folder_path=folder_path)
        except Exception:
            logger.exception("Background folder load failed")

    background_tasks.add_task(work)
    return {"job_id": jid, "status": "started", "folder": folder_path.name}


@app.get("/folder", response_class=HTMLResponse)
async def folder_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "folder.html",
        {
            "request": request,
            "title": "Load from data folder",
        },
    )
