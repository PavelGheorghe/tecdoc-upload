import fnmatch
import logging
from pathlib import Path

import paramiko

from tecdoc_etl.config import Settings

logger = logging.getLogger(__name__)


def _host_key_policy(settings: Settings) -> paramiko.MissingHostKeyPolicy:
    v = settings.sftp_host_verify.lower()
    if v in ("accept-new", "yes", "auto"):
        return paramiko.AutoAddPolicy()
    return paramiko.RejectPolicy()


def list_remote_archives(settings: Settings) -> list[str]:
    """List matching archive names under SFTP_REMOTE_DIR (no download)."""
    pattern = settings.sftp_file_glob
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(_host_key_policy(settings))
    client.connect(
        hostname=settings.sftp_host,
        port=settings.sftp_port,
        username=settings.sftp_user,
        password=settings.sftp_password or None,
        allow_agent=False,
        look_for_keys=False,
        timeout=60,
    )
    try:
        sftp = client.open_sftp()
        try:
            names = sftp.listdir(settings.sftp_remote_dir)
            matches = sorted(
                n for n in names if fnmatch.fnmatch(n, pattern) and not n.startswith(".")
            )
            if settings.sftp_max_files is not None:
                matches = matches[: settings.sftp_max_files]
            return matches
        finally:
            sftp.close()
    finally:
        client.close()


def download_taf_archives(
    settings: Settings,
    local_download_dir: Path,
    *,
    on_progress=None,
) -> list[Path]:
    """
    List SFTP_REMOTE_DIR for SFTP_FILE_GLOB, optionally limit count, download to local dir.
    Returns sorted list of local .7z paths.
    """
    local_download_dir.mkdir(parents=True, exist_ok=True)
    pattern = settings.sftp_file_glob
    remote_dir = settings.sftp_remote_dir.rstrip("/")

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(_host_key_policy(settings))
    client.connect(
        hostname=settings.sftp_host,
        port=settings.sftp_port,
        username=settings.sftp_user,
        password=settings.sftp_password or None,
        allow_agent=False,
        look_for_keys=False,
        timeout=60,
    )
    try:
        sftp = client.open_sftp()
        try:
            try:
                names = sftp.listdir(settings.sftp_remote_dir)
            except OSError as e:
                raise RuntimeError(
                    f"Cannot access remote directory {settings.sftp_remote_dir!r}: {e}"
                ) from e
            matches = sorted(
                n for n in names if fnmatch.fnmatch(n, pattern) and not n.startswith(".")
            )
            if settings.sftp_max_files is not None:
                matches = matches[: settings.sftp_max_files]
            out: list[Path] = []
            for name in matches:
                remote_path = f"{remote_dir}/{name}"
                local = local_download_dir / name
                if on_progress:
                    on_progress(f"Downloading {name} …")
                logger.info("SFTP get %s -> %s", remote_path, local)
                sftp.get(remote_path, str(local))
                out.append(local)
            return out
        finally:
            sftp.close()
    finally:
        client.close()
