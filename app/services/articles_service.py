import logging
import os
from datetime import datetime

from app.services.file_manager import cleanup_temp, safe_copy_tables
from app.services.paradox_reader import read_table
from app.utils.date_utils import localize_naive

logger = logging.getLogger(__name__)

REQUIRED_TABLES = ["ARTICLES", "CLASSIFICATION"]

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


def _build_category_map(cls_rows: list[dict]) -> dict[int, str]:
    """Build CLS_ID -> category name mapping from CLASSIFICATION table."""
    categories = {}
    for cls in cls_rows:
        cid = cls.get("CLS_ID")
        if cid is None:
            continue
        try:
            cid_int = int(float(cid))
        except (ValueError, TypeError):
            continue

        # Try common Pointex column name patterns
        cname = ""
        for key_pattern in ["CLS_CLASSIFICATION", "CLS_LIBELLE", "CLS_NOM", "CLS_DESIGN"]:
            val = cls.get(key_pattern)
            if val:
                cname = str(val).strip()
                break

        # Fallback: first string column that's not CLS_ID
        if not cname:
            for k, v in cls.items():
                if k.upper() != "CLS_ID" and isinstance(v, str) and v.strip():
                    cname = v.strip()
                    break

        if cid_int != 0 and cname:
            categories[cid_int] = cname
            categories[abs(cid_int)] = cname  # sign-bit handling

    logger.info("CLASSIFICATION: %d categories loaded", len(categories))
    return categories


def _resolve_category(art: dict, categories_map: dict[int, str]) -> str | None:
    """Resolve article's CLS_ID to a category name."""
    cls_id = art.get("CLS_ID")
    if cls_id is None:
        return None
    try:
        cls_id_int = int(float(cls_id))
        cat = categories_map.get(cls_id_int)
        if not cat and cls_id_int < 0:
            cat = categories_map.get(abs(cls_id_int))
        return cat
    except (ValueError, TypeError):
        return None


def _article_to_dict(art: dict, mod_col: str | None, categories_map: dict[int, str]) -> dict:
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
        "category": _resolve_category(art, categories_map),
        "sellingPrice": None,
        "costPrice": art.get("ART_DEF_PMPA"),
        "unit": "piece",
        "isActive": cache == 0 and valide != 0,
        "updatedAt": mod_date.isoformat() if mod_date else None,
    }


def get_articles(updated_since: datetime | None = None) -> dict:
    """Read the ARTICLES table and return the catalog."""
    tmp_dir = safe_copy_tables(REQUIRED_TABLES)
    try:
        rows = read_table(os.path.join(tmp_dir, "ARTICLES.DB"))

        # Build category map from CLASSIFICATION
        categories_map = {}
        cls_path = os.path.join(tmp_dir, "CLASSIFICATION.DB")
        if os.path.isfile(cls_path):
            try:
                cls_rows = read_table(cls_path)
                categories_map = _build_category_map(cls_rows)
            except Exception as e:
                logger.warning("Could not read CLASSIFICATION: %s", e)

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

            articles.append(_article_to_dict(art, mod_col, categories_map))

        return {"articles": articles, "totalCount": len(articles)}

    finally:
        cleanup_temp(tmp_dir)


def get_article_by_id(article_id: int) -> dict | None:
    """Read the ARTICLES table and return a single article by ART_ID."""
    tmp_dir = safe_copy_tables(REQUIRED_TABLES)
    try:
        rows = read_table(os.path.join(tmp_dir, "ARTICLES.DB"))

        # Build category map
        categories_map = {}
        cls_path = os.path.join(tmp_dir, "CLASSIFICATION.DB")
        if os.path.isfile(cls_path):
            try:
                cls_rows = read_table(cls_path)
                categories_map = _build_category_map(cls_rows)
            except Exception as e:
                logger.warning("Could not read CLASSIFICATION: %s", e)

        mod_col = _find_col_name(rows, _MODIFIED_PREFIX)

        for art in rows:
            if art.get("ART_ID") == article_id:
                return _article_to_dict(art, mod_col, categories_map)

        return None

    finally:
        cleanup_temp(tmp_dir)
