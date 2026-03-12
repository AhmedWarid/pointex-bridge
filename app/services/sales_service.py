import logging
import os
from collections import defaultdict
from datetime import datetime

from app.services.file_manager import cleanup_temp, safe_copy_tables
from app.services.paradox_reader import read_table
from app.utils.date_utils import is_in_period

logger = logging.getLogger(__name__)

REQUIRED_TABLES = ["VENTE_REGLEE", "ARTICLE_VENDU", "ARTICLES"]

# Column name prefixes — Paradox truncates long names, so we match by prefix
_DATE_COL_PREFIX = "VTE_DATE_REGLE"


def _find_col(row: dict, prefix: str):
    """Find the first column whose name starts with prefix."""
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


def get_sales(from_dt: datetime, to_dt: datetime) -> dict:
    """
    Read Paradox tables, filter sales by date, aggregate by article.
    Returns dict matching the ProtoCart SalesResponse schema.
    """
    tmp_dir = safe_copy_tables(REQUIRED_TABLES)
    try:
        # Read tables
        ventes = read_table(os.path.join(tmp_dir, "VENTE_REGLEE.DB"))
        lignes = read_table(os.path.join(tmp_dir, "ARTICLE_VENDU.DB"))
        articles_raw = read_table(os.path.join(tmp_dir, "ARTICLES.DB"))

        # Build articles lookup
        articles_map = {}
        for art in articles_raw:
            art_id = art.get("ART_ID")
            if art_id is not None:
                articles_map[art_id] = art

        # Filter VENTE_REGLEE by date
        date_col = _find_col_name(ventes, _DATE_COL_PREFIX)
        valid_vte_ids = set()
        for row in ventes:
            rec_date = row.get(date_col) if date_col else None
            if is_in_period(rec_date, from_dt, to_dt):
                vte_id = row.get("VTE_ID")
                if vte_id is not None:
                    valid_vte_ids.add(vte_id)

        logger.info(
            "Found %d transactions in period %s -> %s",
            len(valid_vte_ids),
            from_dt.isoformat(),
            to_dt.isoformat(),
        )

        # Aggregate ARTICLE_VENDU by ART_ID
        agg = defaultdict(
            lambda: {
                "quantitySold": 0.0,
                "totalRevenue": 0.0,
                "unitPrice": 0.0,
                "transactions": set(),
            }
        )

        for line in lignes:
            vte_id = line.get("VTE_ID")
            if vte_id not in valid_vte_ids:
                continue
            if line.get("VTE_CACHE", 0) not in (0, None):
                continue

            art_id = line.get("ART_ID")
            if art_id is None:
                continue

            qty = line.get("VTE_QUANTITE", 0) or 0
            pvht = line.get("VTE_PVHT", 0) or 0

            agg[art_id]["quantitySold"] += qty
            agg[art_id]["totalRevenue"] += pvht * qty
            agg[art_id]["unitPrice"] = pvht
            agg[art_id]["transactions"].add(vte_id)

        # Build response
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
