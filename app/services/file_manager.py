import logging
import os
import shutil
import tempfile
import time
from datetime import date

from app.config import settings

logger = logging.getLogger(__name__)

# Copy ALL companion files — not just .DB and .PX
# Paradox uses many extensions and the reader may need any of them
PARADOX_EXTENSIONS = [
    ".DB", ".PX", ".MB", ".XG0", ".XG1", ".XG2", ".XG3",
    ".YG0", ".YG1", ".YG2", ".YG3", ".VAL", ".TV", ".FAM",
    ".X0?", ".Y0?",
]


def _find_companion_files(table_name: str, source_dir: str) -> list[str]:
    """
    Find ALL files in source_dir that belong to a Paradox table.
    Matches by table name prefix with any extension.
    e.g. for "ARTICLES" finds ARTICLES.DB, ARTICLES.PX, ARTICLES.MB,
    ARTICLES.XG0, ARTICLES.XG1, ARTICLES.XG2, ARTICLES.YG0, etc.
    """
    prefix = table_name.upper() + "."
    files = []
    try:
        for f in os.listdir(source_dir):
            if f.upper().startswith(prefix):
                files.append(f)
    except Exception as e:
        logger.warning("Cannot list directory %s: %s", source_dir, e)
    return files


def safe_copy_tables(table_names: list[str]) -> str:
    """
    Copy Paradox table files to a temp directory to avoid lock conflicts
    with the running POS software.

    Copies ALL files matching each table name (e.g. ARTICLES.DB, ARTICLES.PX,
    ARTICLES.MB, ARTICLES.XG0, ARTICLES.XG1, etc.)

    Retries up to 3 times per file on PermissionError.
    Falls back to reading directly from the share if copy fails.

    Returns the path to read from (temp dir or source dir).
    """
    tmp_dir = tempfile.mkdtemp(prefix="pointex_")
    copy_ok = True

    for table in table_names:
        companion_files = _find_companion_files(table, settings.saveurs_path)
        if not companion_files:
            logger.warning("No files found for table %s in %s", table, settings.saveurs_path)
            copy_ok = False
            continue

        logger.info("Copying %s: %s", table, ", ".join(companion_files))

        for fname in companion_files:
            src = os.path.join(settings.saveurs_path, fname)
            dst = os.path.join(tmp_dir, fname)
            success = False

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
                    else:
                        success = True
                    break
                except PermissionError:
                    logger.warning(
                        "File locked: %s (attempt %d/3)", src, attempt + 1
                    )
                    if attempt < 2:
                        time.sleep(1)
                except Exception as e:
                    logger.warning("Error copying %s: %s", src, e)
                    break

            if not success and fname.upper().endswith(".DB"):
                copy_ok = False

    # If critical files failed, fall back to reading directly from the share
    if not copy_ok:
        logger.warning(
            "Some files could not be copied properly. "
            "Falling back to reading directly from %s",
            settings.saveurs_path,
        )
        cleanup_temp(tmp_dir)
        return settings.saveurs_path

    return tmp_dir


def get_archive_paths(target_date: date) -> tuple[str | None, str | None]:
    """
    Return (vd_path, ve_path) for a date's daily archive files.
    VD = detail lines, VE = receipt headers.
    Returns None for each file that doesn't exist.
    Archive files are static (written after Z closing) so no safe_copy needed.
    """
    year_folder = f"AN{target_date.year}"
    vd_name = f"VD{target_date.strftime('%m%d%y')}.DB"
    ve_name = f"VE{target_date.strftime('%m%d%y')}.DB"
    vd_path = os.path.join(settings.saveurs_path, year_folder, vd_name)
    ve_path = os.path.join(settings.saveurs_path, year_folder, ve_name)
    return (
        vd_path if os.path.isfile(vd_path) else None,
        ve_path if os.path.isfile(ve_path) else None,
    )


def cleanup_temp(tmp_dir: str):
    """Remove a temp directory created by safe_copy_tables."""
    if tmp_dir == settings.saveurs_path:
        return
    try:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    except Exception:
        logger.warning("Failed to clean up temp dir: %s", tmp_dir)
