from datetime import datetime
from zoneinfo import ZoneInfo

from app.config import settings


def get_tz():
    return ZoneInfo(settings.timezone)


def parse_iso(dt_str: str) -> datetime:
    """Parse an ISO 8601 datetime string into a timezone-aware datetime."""
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=get_tz())
    return dt


def localize_naive(dt: datetime) -> datetime:
    """Attach the configured timezone to a naive datetime (from Paradox)."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=get_tz())
    return dt


def is_in_period(record_date, from_dt: datetime, to_dt: datetime) -> bool:
    if record_date is None:
        return False
    record_date = localize_naive(record_date)
    return from_dt <= record_date <= to_dt
