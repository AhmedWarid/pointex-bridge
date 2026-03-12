import logging
import os
from datetime import datetime

from app.services.file_manager import cleanup_temp, safe_copy_tables
from app.services.paradox_reader import read_table
from app.utils.date_utils import localize_naive

logger = logging.getLogger(__name__)

REQUIRED_TABLES = ["ARTICLES"]

# Column name prefixes for truncated Paradox column names
_MODIFIED_PREFIX = "ART_DATE_MODI"
_CREATED_PREFIX = "ART_DATE_CREA"


def _find_col(row: dict, prefix: str):
    for key in row:
        if key.upper().startswith(prefix.upper()):
            return row[key]
    return None


def _find_col_name(rows: list[dict], prefix: str) -> str | None:
    if not rows:
        return None
    for key in rows[0]:
        if key.upper().startswith(prefix.upper()):
            return key
    return None


def _article_to_dict(art: dict, mod_col: str | None) -> dict:
    """Convert a raw Paradox article row to the API response shape."""
    mod_date = art.get(mod_col) if mod_col else None
    if mod_date is not None:
        mod_date = localize_naive(mod_date)

    cache = art.get("ART_CACHE", 0) or 0
    valide = art.get("ART_VALIDE", 1)

    return {
        "posArticleId": str(art.get("ART_ID", "")),
        "barcode": art.get("ART_BARCODE") or None,
        "name": art.get("ART_ARTICLE", ""),
        "category": None,  # V2: resolve ART_CATEGORIE via lookup table
        "sellingPrice": None,  # V2: resolve from TARIFS or custom fields
        "costPrice": art.get("ART_DEF_PMPA"),
        "unit": "piece",  # V2: resolve from UNITES.DB
        "isActive": cache == 0 and valide != 0,
        "updatedAt": mod_date.isoformat() if mod_date else None,
    }


def get_articles(updated_since: datetime | None = None) -> dict:
    """Read the ARTICLES table and return the catalog."""
    tmp_dir = safe_copy_tables(REQUIRED_TABLES)
    try:
        rows = read_table(os.path.join(tmp_dir, "ARTICLES.DB"))

        mod_col = _find_col_name(rows, _MODIFIED_PREFIX)

        articles = []
        for art in rows:
            # Filter by updatedSince if provided
            if updated_since is not None and mod_col:
                mod_date = art.get(mod_col)
                if mod_date is not None:
                    mod_date = localize_naive(mod_date)
                    if mod_date <= updated_since:
                        continue

            articles.append(_article_to_dict(art, mod_col))

        return {"articles": articles, "totalCount": len(articles)}

    finally:
        cleanup_temp(tmp_dir)


def get_article_by_id(article_id: int) -> dict | None:
    """Read the ARTICLES table and return a single article by ART_ID."""
    tmp_dir = safe_copy_tables(REQUIRED_TABLES)
    try:
        rows = read_table(os.path.join(tmp_dir, "ARTICLES.DB"))
        mod_col = _find_col_name(rows, _MODIFIED_PREFIX)

        for art in rows:
            if art.get("ART_ID") == article_id:
                return _article_to_dict(art, mod_col)

        return None

    finally:
        cleanup_temp(tmp_dir)
