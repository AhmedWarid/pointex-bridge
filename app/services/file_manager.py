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

    If a file copies as 0 bytes (locked by POS), falls back to reading
    directly from the network share.

    Returns the path to the temp directory (or the source dir if copy fails).
    """
    tmp_dir = tempfile.mkdtemp(prefix="pointex_")
    copy_ok = True

    for table in table_names:
        for ext in PARADOX_EXTENSIONS:
            src = os.path.join(settings.saveurs_path, f"{table}{ext}")
            if not os.path.exists(src):
                continue
            dst = os.path.join(tmp_dir, f"{table}{ext}")
            for attempt in range(3):
                try:
                    shutil.copy2(src, dst)
                    # Verify copy is not empty when source is not
                    src_size = os.path.getsize(src)
                    dst_size = os.path.getsize(dst)
                    if dst_size == 0 and src_size > 0:
                        logger.warning(
                            "File copied as 0 bytes (locked?): %s (%d bytes source)",
                            src, src_size,
                        )
                        copy_ok = False
                    break
                except PermissionError:
                    logger.warning(
                        "File locked: %s (attempt %d/3)", src, attempt + 1
                    )
                    if attempt == 2:
                        logger.warning("Giving up on copying %s after 3 attempts", src)
                        copy_ok = False
                    else:
                        time.sleep(1)
                except Exception as e:
                    logger.warning("Error copying %s: %s", src, e)
                    copy_ok = False
                    break

    # If any copy failed, fall back to reading directly from the share
    if not copy_ok:
        logger.warning(
            "Some files could not be copied properly. "
            "Falling back to reading directly from %s. "
            "This may be slower or fail if files are actively being written.",
            settings.saveurs_path,
        )
        cleanup_temp(tmp_dir)
        return settings.saveurs_path

    return tmp_dir


def cleanup_temp(tmp_dir: str):
    """Remove a temp directory created by safe_copy_tables."""
    # Don't delete the source directory!
    if tmp_dir == settings.saveurs_path:
        return
    try:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    except Exception:
        logger.warning("Failed to clean up temp dir: %s", tmp_dir)
