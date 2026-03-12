import os
from datetime import datetime

from fastapi import APIRouter

from app.config import settings

router = APIRouter(prefix="/api/pos", tags=["health"])

EXPECTED_TABLES = ["ARTICLES", "VENTE_REGLEE", "ARTICLE_VENDU", "CAISSE_ES"]


@router.get("/health")
async def health_check():
    """Check that the bridge can reach the Paradox files on CAISSE-PC."""
    path_accessible = os.path.isdir(settings.saveurs_path)

    tables_found = []
    last_modified = None

    if path_accessible:
        for table in EXPECTED_TABLES:
            db_file = os.path.join(settings.saveurs_path, f"{table}.DB")
            if os.path.exists(db_file):
                tables_found.append(table)

        articles_path = os.path.join(settings.saveurs_path, "ARTICLES.DB")
        if os.path.exists(articles_path):
            mtime = os.path.getmtime(articles_path)
            last_modified = datetime.fromtimestamp(mtime).isoformat()

    status = "ok" if path_accessible and len(tables_found) >= 2 else "degraded"
    if not path_accessible:
        status = "error"

    return {
        "status": status,
        "saveurs_path_accessible": path_accessible,
        "last_articles_modified": last_modified,
        "tables_found": tables_found,
    }
