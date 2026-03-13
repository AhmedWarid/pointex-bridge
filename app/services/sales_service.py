import logging
import os
from collections import defaultdict
from datetime import datetime

from app.services.file_manager import cleanup_temp, safe_copy_tables
from app.services.paradox_reader import read_table
from app.utils.date_utils import is_in_period

logger = logging.getLogger(__name__)

# --- Pointex data model ---
# NOTE_ENTETE  = receipt headers  (VTE_ID, dates, totals, status)
# NOTE_DETAIL  = line items       (VTE_ID FK, ART_ID FK, qty, price)
# ARTICLES     = product catalog  (ART_ID, name, barcode)
#
# VENTE_REGLEE / ARTICLE_VENDU are PURGED after daily closing (cloture).
# The live data is always in NOTE_ENTETE + NOTE_DETAIL.

REQUIRED_TABLES = ["NOTE_ENTETE", "NOTE_DETAIL", "ARTICLES"]

# Column name prefixes for flexible matching (Paradox truncates long names)
_DATE_COL_PREFIX = "VTE_DATE_DE_LA"  # VTE_DATE_DE_LA_PIECE


def _find_col(row: dict, prefix: str):
    """Find the first column value whose name starts with prefix."""
    for key in row:
        if key.upper().startswith(prefix.upper()):
            return row[key]
    return None


def _find_col_name(rows: list[dict], prefix: str) -> str | None:
    """Find the actual column name matching a prefix from the first row."""
    if not rows:
        return None
    for key in rows[0]:
        if key.upper().startswith(prefix.upper()):
            return key
    return None


def _normalize_vte_id(val) -> int | None:
    """
    Normalize VTE_ID to int for reliable joins.
    Paradox stores these as floats (e.g. 1024068666.00).
    """
    if val is None:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError, OverflowError):
        return None


def get_sales(from_dt: datetime, to_dt: datetime) -> dict:
    """
    Read Paradox tables, filter sales by date, aggregate by article.
    Returns dict matching the ProtoCart SalesResponse schema.
    """
    tmp_dir = safe_copy_tables(REQUIRED_TABLES)
    try:
        # Read tables
        entetes = read_table(os.path.join(tmp_dir, "NOTE_ENTETE.DB"))
        details = read_table(os.path.join(tmp_dir, "NOTE_DETAIL.DB"))
        articles_raw = read_table(os.path.join(tmp_dir, "ARTICLES.DB"))

        # Build articles lookup  (ART_ID -> article row)
        articles_map = {}
        for art in articles_raw:
            art_id = art.get("ART_ID")
            if art_id is not None:
                articles_map[_normalize_vte_id(art_id) or art_id] = art

        # --- Filter NOTE_ENTETE by date ---
        date_col = _find_col_name(entetes, _DATE_COL_PREFIX)
        if not date_col:
            # Fallback: try any column with "DATE" in it
            date_col = _find_col_name(entetes, "VTE_DATE")

        valid_vte_ids = set()
        entete_totals = {}  # VTE_ID -> total TTC

        for row in entetes:
            # Note: VTE_NB_ANNULE = number of voided LINES, not "receipt cancelled"
            # VTE_CACHE = 1 means the entire receipt is hidden/voided
            vte_cache = row.get("VTE_CACHE")
            if vte_cache is not None:
                try:
                    if int(float(vte_cache)) != 0:
                        continue
                except (ValueError, TypeError):
                    pass

            rec_date = row.get(date_col) if date_col else None
            if is_in_period(rec_date, from_dt, to_dt):
                vte_id = _normalize_vte_id(row.get("VTE_ID"))
                if vte_id is not None:
                    valid_vte_ids.add(vte_id)
                    # Capture the receipt total for reference
                    total_ttc = _find_col(row, "VTE_TOTAL_TTC")
                    if total_ttc is not None:
                        try:
                            entete_totals[vte_id] = float(total_ttc)
                        except (ValueError, TypeError):
                            pass

        logger.info(
            "Found %d receipts in period %s -> %s (date column: %s)",
            len(valid_vte_ids),
            from_dt.isoformat(),
            to_dt.isoformat(),
            date_col or "(not found)",
        )

        if not valid_vte_ids and entetes:
            # Log sample dates for debugging
            sample_dates = []
            for row in entetes[:5]:
                d = row.get(date_col) if date_col else None
                sample_dates.append(str(d))
            logger.warning(
                "No receipts matched the date filter. "
                "Sample dates from NOTE_ENTETE: %s",
                sample_dates,
            )

        # --- Aggregate NOTE_DETAIL by ART_ID ---
        agg = defaultdict(
            lambda: {
                "quantitySold": 0.0,
                "totalRevenue": 0.0,
                "unitPrice": 0.0,
                "transactions": set(),
            }
        )

        for line in details:
            vte_id = _normalize_vte_id(line.get("VTE_ID"))
            if vte_id not in valid_vte_ids:
                continue

            # Skip voided / hidden lines
            vte_cache = line.get("VTE_CACHE")
            if vte_cache is not None:
                try:
                    if int(float(vte_cache)) != 0:
                        continue
                except (ValueError, TypeError):
                    pass

            # Skip non-article lines (subtotals, payments, separators)
            art_id = line.get("ART_ID")
            if art_id is None:
                continue
            art_id_norm = _normalize_vte_id(art_id)
            if art_id_norm is None or art_id_norm == 0:
                continue

            qty = 0
            raw_qty = line.get("VTE_QUANTITE")
            if raw_qty is not None:
                try:
                    qty = float(raw_qty)
                except (ValueError, TypeError):
                    pass

            # Selling price — try VTE_PRIX_DE_VENTE first, then VTE_PVHT
            price = 0
            for price_col in ("VTE_PRIX_DE_VENTE", "VTE_PRIX_DE_V", "VTE_PVHT"):
                raw_price = _find_col(line, price_col)
                if raw_price is not None:
                    try:
                        price = float(raw_price)
                        break
                    except (ValueError, TypeError):
                        pass

            # Handle discount
            remise = 0
            raw_remise = line.get("VTE_REMISE")
            if raw_remise is not None:
                try:
                    remise = float(raw_remise)
                except (ValueError, TypeError):
                    pass

            effective_price = price * (1 - remise / 100) if remise else price

            agg[art_id_norm]["quantitySold"] += qty
            agg[art_id_norm]["totalRevenue"] += effective_price * qty
            agg[art_id_norm]["unitPrice"] = price
            agg[art_id_norm]["transactions"].add(vte_id)

        # --- Build response ---
        sales = []
        total_revenue = 0.0
        all_transactions = set()

        for art_id, data in agg.items():
            article = articles_map.get(art_id, {})
            revenue = round(data["totalRevenue"], 2)
            total_revenue += revenue
            all_transactions.update(data["transactions"])

            sales.append(
                {
                    "posArticleId": str(art_id),
                    "barcode": article.get("ART_BARCODE") or None,
                    "articleName": article.get("ART_ARTICLE", f"Unknown ({art_id})"),
                    "quantitySold": round(data["quantitySold"], 3),
                    "weightSoldKg": None,  # V2: check unit type
                    "totalRevenue": revenue,
                    "unitPrice": round(data["unitPrice"], 2),
                    "transactionCount": len(data["transactions"]),
                }
            )

        now = datetime.now().astimezone()
        return {
            "sales": sales,
            "metadata": {
                "periodFrom": from_dt.isoformat(),
                "periodTo": to_dt.isoformat(),
                "totalTransactions": len(all_transactions),
                "totalRevenue": round(total_revenue, 2),
                "generatedAt": now.isoformat(),
            },
        }

    finally:
        cleanup_temp(tmp_dir)
