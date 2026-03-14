"""
Sales service — combines live Paradox data with FC2 journal history.

Data sources:
  1. NOTE_ENTETE + NOTE_DETAIL (Paradox): Today's active/open receipts
  2. FC2 journal files (JV*.TXT):        Historical closed sales

The bridge tries FC2 first for historical data, and falls back to
live Paradox tables for today's active receipts.
"""

import logging
import os
from collections import defaultdict
from datetime import datetime

from app.config import settings
from app.services.fc2_reader import (
    get_journal_sales,
    list_fc2_files,
)
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


# ── FC2 journal-based sales ──────────────────────────────────────────────


def _aggregate_journal_lines(lines: list[dict]) -> dict:
    """
    Aggregate flat journal sale lines into the ProtoCart response format.
    Each line has: article_code, article_name, quantity, unit_price,
    discount_pct, receipt_id, classification, etc.
    """
    agg = defaultdict(
        lambda: {
            "quantitySold": 0.0,
            "totalRevenue": 0.0,
            "unitPrice": 0.0,
            "transactions": set(),
            "articleName": "",
            "classification": "",
        }
    )

    for line in lines:
        code = line["article_code"]
        if not code:
            continue

        qty = line["quantity"]
        price = line["unit_price"]
        discount = line["discount_pct"]
        effective_price = price * (1 - discount / 100) if discount else price

        agg[code]["quantitySold"] += qty
        agg[code]["totalRevenue"] += effective_price * qty
        agg[code]["unitPrice"] = price
        agg[code]["articleName"] = line["article_name"]
        agg[code]["classification"] = line["classification"]
        # receipt_id resets daily, so use date+receipt as unique key
        txn_key = f"{line['date'].date()}_{line['receipt_id']}"
        agg[code]["transactions"].add(txn_key)

    sales = []
    total_revenue = 0.0
    all_transactions = set()

    for art_code, data in agg.items():
        revenue = round(data["totalRevenue"], 2)
        total_revenue += revenue
        all_transactions.update(data["transactions"])

        sales.append({
            "posArticleId": str(art_code),
            "barcode": None,
            "articleName": data["articleName"],
            "quantitySold": round(data["quantitySold"], 3),
            "weightSoldKg": None,
            "totalRevenue": revenue,
            "unitPrice": round(data["unitPrice"], 2),
            "transactionCount": len(data["transactions"]),
            "classification": data["classification"] or None,
        })

    return {
        "sales": sales,
        "totalRevenue": round(total_revenue, 2),
        "totalTransactions": len(all_transactions),
    }


def _get_fc2_sales(from_dt: datetime, to_dt: datetime) -> dict | None:
    """
    Try to get sales from the most recent FC2 file.
    Returns aggregated result dict or None if no FC2 files found.
    """
    fc2_dir = settings.fc2_dir
    fc2_files = list_fc2_files(fc2_dir)

    if not fc2_files:
        logger.info("No FC2 files found in %s", fc2_dir)
        return None

    # Use the most recent FC2 file
    fc2_path = fc2_files[0]
    logger.info("Using FC2 file: %s", fc2_path)

    try:
        lines = get_journal_sales(fc2_path, from_dt, to_dt)
    except Exception as e:
        logger.warning("Failed to read FC2 file %s: %s", fc2_path, e)
        return None

    if not lines:
        logger.info("No journal lines found in FC2 for the requested period")
        return None

    return _aggregate_journal_lines(lines)


# ── Live Paradox-based sales ─────────────────────────────────────────────


def _get_live_sales(from_dt: datetime, to_dt: datetime) -> dict:
    """
    Read live Paradox tables (NOTE_ENTETE + NOTE_DETAIL) for active receipts.
    This is the original approach — works for today's open tickets.
    """
    tmp_dir = safe_copy_tables(REQUIRED_TABLES)
    try:
        entetes = read_table(os.path.join(tmp_dir, "NOTE_ENTETE.DB"))
        details = read_table(os.path.join(tmp_dir, "NOTE_DETAIL.DB"))
        articles_raw = read_table(os.path.join(tmp_dir, "ARTICLES.DB"))

        # Build articles lookup
        articles_map = {}
        for art in articles_raw:
            art_id = art.get("ART_ID")
            if art_id is not None:
                articles_map[_normalize_vte_id(art_id) or art_id] = art

        # Filter NOTE_ENTETE by date
        date_col = _find_col_name(entetes, _DATE_COL_PREFIX)
        if not date_col:
            date_col = _find_col_name(entetes, "VTE_DATE")

        valid_vte_ids = set()

        for row in entetes:
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

        logger.info(
            "Live Paradox: %d receipts in period (date column: %s)",
            len(valid_vte_ids),
            date_col or "(not found)",
        )

        # Aggregate NOTE_DETAIL
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

            vte_cache = line.get("VTE_CACHE")
            if vte_cache is not None:
                try:
                    if int(float(vte_cache)) != 0:
                        continue
                except (ValueError, TypeError):
                    pass

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

            price = 0
            for price_col in ("VTE_PRIX_DE_VENTE", "VTE_PRIX_DE_V", "VTE_PVHT"):
                raw_price = _find_col(line, price_col)
                if raw_price is not None:
                    try:
                        price = float(raw_price)
                        break
                    except (ValueError, TypeError):
                        pass

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

        sales = []
        total_revenue = 0.0
        all_transactions = set()

        for art_id, data in agg.items():
            article = articles_map.get(art_id, {})
            revenue = round(data["totalRevenue"], 2)
            total_revenue += revenue
            all_transactions.update(data["transactions"])

            sales.append({
                "posArticleId": str(art_id),
                "barcode": article.get("ART_BARCODE") or None,
                "articleName": article.get("ART_ARTICLE", f"Unknown ({art_id})"),
                "quantitySold": round(data["quantitySold"], 3),
                "weightSoldKg": None,
                "totalRevenue": revenue,
                "unitPrice": round(data["unitPrice"], 2),
                "transactionCount": len(data["transactions"]),
                "classification": None,
            })

        return {
            "sales": sales,
            "totalRevenue": round(total_revenue, 2),
            "totalTransactions": len(all_transactions),
        }
    finally:
        cleanup_temp(tmp_dir)


# ── Public API ───────────────────────────────────────────────────────────


def get_sales(from_dt: datetime, to_dt: datetime) -> dict:
    """
    Get aggregated sales per product for a given period.

    Strategy:
      1. Try FC2 journals first (historical closed sales)
      2. Also check live Paradox (today's active receipts)
      3. Merge both sources (FC2 has closed data, Paradox has open tickets)

    Returns dict matching the ProtoCart SalesResponse schema.
    """
    fc2_result = None
    live_result = None
    source = "none"

    # Try FC2 journals for historical data
    try:
        fc2_result = _get_fc2_sales(from_dt, to_dt)
        if fc2_result and fc2_result["sales"]:
            source = "fc2"
            logger.info(
                "FC2: %d articles, %.2f DH revenue",
                len(fc2_result["sales"]),
                fc2_result["totalRevenue"],
            )
    except Exception as e:
        logger.warning("FC2 read failed: %s", e)

    # Try live Paradox for today's active receipts
    try:
        live_result = _get_live_sales(from_dt, to_dt)
        if live_result and live_result["sales"]:
            if source == "fc2":
                source = "fc2+live"
            else:
                source = "live"
            logger.info(
                "Live: %d articles, %.2f DH revenue",
                len(live_result["sales"]),
                live_result["totalRevenue"],
            )
    except Exception as e:
        logger.warning("Live Paradox read failed: %s", e)

    # Merge results
    merged_sales = {}  # key -> sale dict
    total_transactions = set()

    for result in [fc2_result, live_result]:
        if not result or not result["sales"]:
            continue
        for sale in result["sales"]:
            key = sale["posArticleId"]
            if key in merged_sales:
                existing = merged_sales[key]
                existing["quantitySold"] += sale["quantitySold"]
                existing["totalRevenue"] += sale["totalRevenue"]
                existing["transactionCount"] += sale["transactionCount"]
                # Keep the higher unit price (more recent)
                if sale["unitPrice"] > existing["unitPrice"]:
                    existing["unitPrice"] = sale["unitPrice"]
                # Fill in missing fields
                if not existing.get("articleName") or existing["articleName"].startswith("Unknown"):
                    existing["articleName"] = sale["articleName"]
                if not existing.get("classification") and sale.get("classification"):
                    existing["classification"] = sale["classification"]
                if not existing.get("barcode") and sale.get("barcode"):
                    existing["barcode"] = sale["barcode"]
            else:
                merged_sales[key] = dict(sale)

    sales = list(merged_sales.values())
    total_revenue = sum(s["totalRevenue"] for s in sales)

    # Count total unique transactions
    total_txn_count = 0
    if fc2_result:
        total_txn_count += fc2_result["totalTransactions"]
    if live_result:
        total_txn_count += live_result["totalTransactions"]

    now = datetime.now().astimezone()
    return {
        "sales": sales,
        "metadata": {
            "periodFrom": from_dt.isoformat(),
            "periodTo": to_dt.isoformat(),
            "totalTransactions": total_txn_count,
            "totalRevenue": round(total_revenue, 2),
            "generatedAt": now.isoformat(),
            "source": source,
        },
    }
