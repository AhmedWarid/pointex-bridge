import logging

from fastapi import APIRouter, HTTPException, Query

from app.services.sales_service import get_sales
from app.utils.date_utils import parse_iso

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pos", tags=["sales"])


@router.get("/sales")
async def sales(
    from_: str = Query(..., alias="from", description="Period start (ISO 8601)"),
    to: str = Query(..., description="Period end (ISO 8601)"),
):
    """Get aggregated sales per product for a given period."""
    try:
        from_dt = parse_iso(from_)
        to_dt = parse_iso(to)
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=422, detail=f"Invalid date format: {e}")

    try:
        return get_sales(from_dt, to_dt)
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
        logger.exception("Error reading sales data")
        raise HTTPException(status_code=500, detail="Internal error reading sales data")
