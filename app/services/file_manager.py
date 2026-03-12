import logging
import os
import shutil
import tempfile
import time

from app.config import settings

logger = logging.getLogger(__name__)

PARADOX_EXTENSIONS = [".DB", ".PX", ".XG0", ".YG0", ".MB", ".VAL"]


def safe_copy_tables(table_names: list[str]) -> str:
    """
    Copy Paradox table files to a temp directory to avoid lock conflicts
    with the running POS software. Retries up to 3 times per file on
    PermissionError.

    Returns the path to the temp directory.
    """
    tmp_dir = tempfile.mkdtemp(prefix="pointex_")
    for table in table_names:
        for ext in PARADOX_EXTENSIONS:
            src = os.path.join(settings.saveurs_path, f"{table}{ext}")
            if not os.path.exists(src):
                continue
            for attempt in range(3):
                try:
                    shutil.copy2(src, tmp_dir)
                    break
                except PermissionError:
                    logger.warning(
                        "File locked: %s (attempt %d/3)", src, attempt + 1
                    )
                    if attempt == 2:
                        raise
                    time.sleep(1)
    return tmp_dir


def cleanup_temp(tmp_dir: str):
    """Remove a temp directory created by safe_copy_tables."""
    try:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    except Exception:
        logger.warning("Failed to clean up temp dir: %s", tmp_dir)
