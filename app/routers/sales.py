import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from app.config import settings
from app.services.sales_service import get_sales
from app.utils.date_utils import localize_naive, parse_iso

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pos", tags=["sales"])


@router.get("/sales/{date}")
async def sales_by_date(date: str):
    """
    Get aggregated sales for a single date.
    Accepts YYYY-MM-DD format (e.g. 2026-03-15).
    """
    logger.info("Sales API request received for single date: %s", date)
    try:
        target = datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid date format: '{date}'. Use YYYY-MM-DD.",
        )

    from_dt = localize_naive(target.replace(hour=0, minute=0, second=0))
    to_dt = localize_naive(target.replace(hour=23, minute=59, second=59))
    logger.info(
        "Sales API resolved single-date request: date=%s from=%s to=%s",
        date,
        from_dt.isoformat(),
        to_dt.isoformat(),
    )

    try:
        result = get_sales(from_dt, to_dt)
        logger.info(
            "Sales API completed single-date request: date=%s source=%s sales=%d txns=%d revenue=%.2f",
            date,
            result["metadata"].get("source"),
            len(result.get("sales", [])),
            result["metadata"].get("totalTransactions"),
            result["metadata"].get("totalRevenue"),
        )
        return result
    except PermissionError:
        raise HTTPException(
            status_code=503,
            detail="POS data unavailable - files are locked by the POS system",
        )
    except FileNotFoundError:
        raise HTTPException(
            status_code=503,
            detail="POS data unavailable - cannot reach CAISSE-PC",
        )
    except Exception:
        logger.exception("Error reading sales data for %s", date)
        raise HTTPException(status_code=500, detail="Internal error reading sales data")


@router.get("/sales")
async def sales(
    from_: str = Query(..., alias="from", description="Period start (ISO 8601)"),
    to: str = Query(..., description="Period end (ISO 8601)"),
):
    """Get aggregated sales per product for a given period."""
    logger.info("Sales API range request received: from=%s to=%s", from_, to)
    try:
        from_dt = parse_iso(from_)
        to_dt = parse_iso(to)
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=422, detail=f"Invalid date format: {e}")

    max_days = settings.max_sales_range_days
    delta = (to_dt - from_dt).days
    if delta < 0:
        raise HTTPException(status_code=422, detail="'from' must be before 'to'")
    if delta > max_days:
        raise HTTPException(
            status_code=422,
            detail=f"Date range too large: {delta} days (max {max_days})",
        )

    logger.info(
        "Sales API resolved range request: from=%s to=%s days=%d",
        from_dt.isoformat(),
        to_dt.isoformat(),
        delta,
    )

    try:
        result = get_sales(from_dt, to_dt)
        logger.info(
            "Sales API completed range request: from=%s to=%s source=%s sales=%d txns=%d revenue=%.2f",
            from_dt.isoformat(),
            to_dt.isoformat(),
            result["metadata"].get("source"),
            len(result.get("sales", [])),
            result["metadata"].get("totalTransactions"),
            result["metadata"].get("totalRevenue"),
        )
        return result
    except PermissionError:
        raise HTTPException(
            status_code=503,
            detail="POS data unavailable - files are locked by the POS system",
        )
    except FileNotFoundError:
        raise HTTPException(
            status_code=503,
            detail="POS data unavailable - cannot reach CAISSE-PC",
        )
    except Exception:
        logger.exception("Error reading sales data")
        raise HTTPException(status_code=500, detail="Internal error reading sales data")
