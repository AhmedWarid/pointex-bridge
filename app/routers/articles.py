import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.services.articles_service import get_article_by_id, get_articles
from app.utils.date_utils import parse_iso

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pos", tags=["articles"])


@router.get("/articles")
async def articles(
    updatedSince: Optional[str] = Query(
        None, description="Only return articles modified after this date (ISO 8601)"
    ),
):
    """Get the product catalog, optionally filtered by last update."""
    updated_since_dt = None
    if updatedSince:
        try:
            updated_since_dt = parse_iso(updatedSince)
        except (ValueError, TypeError) as e:
            raise HTTPException(status_code=422, detail=f"Invalid date format: {e}")

    try:
        return get_articles(updated_since_dt)
    except PermissionError:
        raise HTTPException(
            status_code=503,
            detail="POS data unavailable — files are locked by the POS system",
        )
    except FileNotFoundError:
        raise HTTPException(
            status_code=503,
            detail="POS data unavailable — cannot reach CAISSE-PC",
        )
    except Exception:
        logger.exception("Error reading articles data")
        raise HTTPException(
            status_code=500, detail="Internal error reading articles data"
        )


@router.get("/articles/{posArticleId}")
async def article_by_id(posArticleId: int):
    """Get a single article by its POS article ID."""
    try:
        result = get_article_by_id(posArticleId)
    except PermissionError:
        raise HTTPException(
            status_code=503,
            detail="POS data unavailable — files are locked by the POS system",
        )
    except FileNotFoundError:
        raise HTTPException(
            status_code=503,
            detail="POS data unavailable — cannot reach CAISSE-PC",
        )
    except Exception:
        logger.exception("Error reading article %s", posArticleId)
        raise HTTPException(
            status_code=500, detail="Internal error reading article data"
        )

    if result is None:
        raise HTTPException(status_code=404, detail="Article not found")

    return result
