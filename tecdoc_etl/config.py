import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse, urlunparse

from dotenv import load_dotenv

load_dotenv()


def _parse_log_extracted_lines() -> int:
    """LOG_EXTRACTED_LINES: 0 / unset = off; positive int = first N lines at INFO; all = DEBUG each line."""
    v = os.environ.get("LOG_EXTRACTED_LINES", "").strip().lower()
    if not v or v == "0":
        return 0
    if v == "all":
        return -1
    try:
        return max(0, int(v))
    except ValueError:
        return 0


def _tecdoc_column_positions_csv_path() -> Path:
    env = os.environ.get("TECDOC_COLUMN_STRING_POSITIONS_CSV", "").strip()
    if env:
        return Path(env)
    return Path(__file__).resolve().parent.parent / "documentation" / "tecdoc_column_string_positions.csv"


def _use_tecdoc_csv_positions() -> bool:
    """USE_TECDOC_CSV_POSITIONS: 1 / unset = slice lines per documentation/tecdoc_column_string_positions.csv."""
    v = os.environ.get("USE_TECDOC_CSV_POSITIONS", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def _t030_use_supplier_layout() -> bool:
    """
    T030_USE_SUPPLIER_LAYOUT: when 1/true, table 030 follows USE_TECDOC_CSV_POSITIONS only (reordered
    supplier header after lstrip). Default 0: 030 always uses the 102-byte PDF slices from the CSV
    when that file defines t030 (no leading strip before slicing).
    """
    v = os.environ.get("T030_USE_SUPPLIER_LAYOUT", "0").strip().lower()
    return v in ("1", "true", "yes", "on")


def _normalize_t030_layout() -> str:
    """
    How to parse table 030 lines.

    - supplier: trim leading file padding, then beznr+sprachnr+dlnr+sa+variable bez+lflag (typical export).
    - pdf102: TecDoc PDF 102-byte record (reserviert 1–22, …); line must be exactly 102 characters
      with no extra leading file padding (only the PDF fields, including space-filled reserved).
    """
    raw = os.environ.get("T030_LAYOUT", "supplier").strip().lower().replace("-", "_")
    if raw in ("supplier", "export", "trim"):
        return "supplier"
    if raw in ("pdf", "pdf102", "tecdoc"):
        return "pdf102"
    return "supplier"


def _normalize_d_taf_subdir() -> str:
    """Single path segment under DATA_DIR (default ``D_TAF``)."""
    s = os.environ.get("D_TAF_SUBDIR", "D_TAF").strip().strip("/\\")
    if not s or "/" in s or "\\" in s or s in (".", ".."):
        return "D_TAF"
    return s


def _normalize_row_parse_mode() -> str:
    """Default fixed_width; only delimiter opts into semicolon (FIELD_DELIMITER) splitting."""
    raw = os.environ.get("ROW_PARSE_MODE", "fixed_width").strip().lower().replace("-", "_")
    if raw in ("", "fixed", "fixedwidth", "fixed_width"):
        return "fixed_width"
    if raw == "delimiter":
        return "delimiter"
    return "fixed_width"


def _normalize_database_url(url: str) -> str:
    if not url:
        return url
    if url.startswith("pgsql://"):
        return "postgresql://" + url[len("pgsql://") :]
    return url


@dataclass(frozen=True)
class Settings:
    database_url: str
    sftp_host: str
    sftp_port: int
    sftp_user: str
    sftp_password: str
    sftp_remote_dir: str
    sftp_file_glob: str
    sftp_max_files: int | None
    data_dir: str
    field_delimiter: str
    text_encoding: str
    pg_client_encoding: str
    sync_truncate: bool
    empty_as_null: bool
    strict_tables: bool
    port: int
    sftp_host_verify: str
    # fixed_width: each line is a fixed-width record (widths from DB + int/numeric widths)
    # delimiter: split line by FIELD_DELIMITER (legacy CSV-style)
    row_parse_mode: str
    fixed_width_integer_width: int
    fixed_width_bigint_width: int
    fixed_width_smallint_width: int
    # 0 = off; N > 0 = INFO first N non-empty lines per file; -1 = DEBUG every non-empty line
    log_extracted_lines: int
    # TecDoc PDF → column slice map (see documentation/tecdoc_column_string_positions.csv)
    tecdoc_column_positions_csv: Path
    # 030 Language Descriptions: supplier export vs strict 102-byte PDF record (env T030_LAYOUT)
    t030_layout: str
    # fixed_width: use pos_0based_start/field_length from tecdoc_column_string_positions.csv per table
    use_tecdoc_csv_positions: bool
    # When False (default), t030 uses PDF CSV slices whenever the CSV maps t030, even if USE_TECDOC_CSV_POSITIONS=0
    t030_use_supplier_layout: bool
    # Immediate subfolder of DATA_DIR containing per-supplier extract trees (e.g. data/D_TAF/0001)
    d_taf_subdir: str


def load_settings() -> Settings:
    db = os.environ.get("DATABASE_URL", "")
    return Settings(
        database_url=_normalize_database_url(db),
        sftp_host=os.environ.get("SFTP_HOST", "datapackage.tecalliance.net"),
        sftp_port=int(os.environ.get("SFTP_PORT", "22")),
        sftp_user=os.environ.get("SFTP_USER", ""),
        sftp_password=os.environ.get("SFTP_PASSWORD", ""),
        sftp_remote_dir=os.environ.get("SFTP_REMOTE_DIR", "/D_TAF"),
        sftp_file_glob=os.environ.get("SFTP_FILE_GLOB", "*.7z"),
        sftp_max_files=int(os.environ["SFTP_MAX_FILES"])
        if os.environ.get("SFTP_MAX_FILES")
        else None,
        data_dir=os.environ.get("DATA_DIR", "/data"),
        field_delimiter=os.environ.get("FIELD_DELIMITER", ";"),
        text_encoding=os.environ.get("TEXT_ENCODING", "cp1252"),
        pg_client_encoding=os.environ.get("PG_CLIENT_ENCODING", "WIN1252"),
        sync_truncate=os.environ.get("SYNC_TRUNCATE", "1") == "1",
        empty_as_null=os.environ.get("EMPTY_AS_NULL", "1") == "1",
        strict_tables=os.environ.get("STRICT_TABLES", "0") == "1",
        port=int(os.environ.get("PORT", "3000")),
        sftp_host_verify=os.environ.get("SFTP_HOST_VERIFY", "accept-new"),
        row_parse_mode=_normalize_row_parse_mode(),
        fixed_width_integer_width=int(os.environ.get("FIXED_WIDTH_INTEGER", "9")),
        fixed_width_bigint_width=int(os.environ.get("FIXED_WIDTH_BIGINT", "18")),
        fixed_width_smallint_width=int(os.environ.get("FIXED_WIDTH_SMALLINT", "5")),
        log_extracted_lines=_parse_log_extracted_lines(),
        tecdoc_column_positions_csv=_tecdoc_column_positions_csv_path(),
        t030_layout=_normalize_t030_layout(),
        use_tecdoc_csv_positions=_use_tecdoc_csv_positions(),
        t030_use_supplier_layout=_t030_use_supplier_layout(),
        d_taf_subdir=_normalize_d_taf_subdir(),
    )


def mask_database_url(url: str) -> str | None:
    if not url:
        return None
    try:
        u = urlparse(_normalize_database_url(url))
        if u.password:
            netloc = f"{u.username}:****@{u.hostname}"
            if u.port:
                netloc += f":{u.port}"
        else:
            netloc = u.netloc
        return urlunparse((u.scheme, netloc, u.path, u.params, u.query, u.fragment))
    except Exception:
        return "(invalid URL)"


SCHEMA = "tecdoc_gheorghe"
ETL_SCHEMA = "tecdoc_etl"
