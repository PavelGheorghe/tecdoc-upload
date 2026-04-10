import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


def extract_7z(archive: Path, output_dir: Path, *, on_progress=None) -> None:
    """Extract 7z archive into output_dir (created if needed)."""
    output_dir.mkdir(parents=True, exist_ok=True)
    if on_progress:
        on_progress(f"Extracting {archive.name} …")
    cmd = ["7z", "x", str(archive), f"-o{output_dir}", "-y", "-bb0"]
    logger.info("Running %s", " ".join(cmd))
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        logger.error("7z stderr: %s", r.stderr)
        raise RuntimeError(
            f"7z failed ({r.returncode}) for {archive}: {r.stderr or r.stdout}"
        )
