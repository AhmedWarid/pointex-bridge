"""
Sales service - archive-first strategy for accurate sales data.

Data sources (in priority order):
  1. AN{YYYY}/VD{MMDDYY}.DB - daily archive after Z closing
  2. NOTE_ENTETE + NOTE_DETAIL - live open receipts before closing
"""

import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta

from app.services.file_manager import cleanup_temp, get_archive_paths, safe_copy_tables
from app.services.paradox_reader import read_table
from app.utils.date_utils import is_in_period, localize_naive

logger = logging.getLogger(__name__)

_DATE_COL_PREFIX = "VTE_DATE_DE_LA"
_SALES_SAMPLE_LIMIT = 8


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


def _normalize_id(val) -> int | None:
    """Normalize a Paradox ID to int. Paradox stores these as floats."""
    if val is None:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError, OverflowError):
        return None


def _build_articles_map(tmp_dir: str) -> dict[int, dict]:
    """Build ART_ID -> article row lookup from ARTICLES.DB."""
    articles_map = {}
    art_path = os.path.join(tmp_dir, "ARTICLES.DB")
    if not os.path.isfile(art_path):
        logger.warning("ARTICLES.DB not found in %s", tmp_dir)
        return articles_map

    rows = read_table(art_path)
    for art in rows:
        aid = _normalize_id(art.get("ART_ID"))
        if aid is not None and aid > 0:
            articles_map[aid] = art

    logger.info("ARTICLES loaded: %d products", len(articles_map))
    return articles_map


def _build_category_map(tmp_dir: str) -> dict[int, str]:
    """Build CLS_ID -> category name lookup from CLASSIFICATION.DB."""
    categories = {}
    cls_path = os.path.join(tmp_dir, "CLASSIFICATION.DB")
    if not os.path.isfile(cls_path):
        logger.warning("CLASSIFICATION.DB not found in %s", tmp_dir)
        return categories

    rows = read_table(cls_path)
    for cls in rows:
        cid = _normalize_id(cls.get("CLS_ID"))
        if cid is None or cid == 0:
            continue

        cname = ""
        for key_pattern in ["CLS_CLASSIFICATION", "CLS_LIBELLE", "CLS_NOM", "CLS_DESIGN"]:
            val = cls.get(key_pattern)
            if val:
                cname = str(val).strip()
                break

        if not cname:
            for k, v in cls.items():
                if k.upper() != "CLS_ID" and isinstance(v, str) and v.strip():
                    cname = v.strip()
                    break

        if cname:
            categories[cid] = cname
            categories[abs(cid)] = cname

    logger.info("CLASSIFICATION loaded: %d categories", len(categories))
    return categories


def _resolve_category(art: dict, categories_map: dict[int, str]) -> str | None:
    """Resolve article CLS_ID to a category name."""
    cls_id = _normalize_id(art.get("CLS_ID"))
    if cls_id is None:
        return None
    cat = categories_map.get(cls_id)
    if not cat and cls_id < 0:
        cat = categories_map.get(abs(cls_id))
    return cat


def _empty_skip_stats() -> dict[str, int]:
    return {
        "lines_total": 0,
        "lines_counted": 0,
        "skip_no_art_id": 0,
        "skip_bad_art_id": 0,
        "skip_art_id_zero": 0,
        "skip_type_ligne": 0,
        "skip_cache": 0,
    }


def _summarize_sale_item(sale: dict) -> dict:
    return {
        "posArticleId": sale.get("posArticleId"),
        "articleName": sale.get("articleName"),
        "category": sale.get("category"),
        "quantitySold": sale.get("quantitySold"),
        "totalRevenue": sale.get("totalRevenue"),
        "unitPrice": sale.get("unitPrice"),
        "transactionCount": sale.get("transactionCount"),
    }


def _aggregate_details(
    details: list[dict],
    articles_map: dict[int, dict],
    categories_map: dict[int, str],
    context_label: str,
) -> dict:
    """
    Aggregate detail lines by ART_ID into sales summary.
    """
    agg = defaultdict(lambda: {"qty": 0.0, "revenue": 0.0, "price": 0.0, "txns": set()})
    skip_stats = _empty_skip_stats()
    skip_stats["lines_total"] = len(details)

    for line in details:
        art_id_raw = line.get("ART_ID")
        if art_id_raw is None:
            skip_stats["skip_no_art_id"] += 1
            continue

        art_id = _normalize_id(art_id_raw)
        if art_id is None:
            skip_stats["skip_bad_art_id"] += 1
            continue
        if art_id == 0:
            skip_stats["skip_art_id_zero"] += 1
            continue

        lt = line.get("VTE_TYPE_LIGNE")
        if lt is not None:
            try:
                if int(float(lt)) != 0:
                    skip_stats["skip_type_ligne"] += 1
                    continue
            except (ValueError, TypeError):
                pass

        vc = line.get("VTE_CACHE")
        if vc is not None:
            try:
                if int(float(vc)) != 0:
                    skip_stats["skip_cache"] += 1
                    continue
            except (ValueError, TypeError):
                pass

        qty = 0.0
        raw_qty = line.get("VTE_QUANTITE")
        if raw_qty is not None:
            try:
                qty = float(raw_qty)
            except (ValueError, TypeError):
                pass

        price = 0.0
        raw_price = line.get("VTE_PRIX_DE_VENTE")
        if raw_price is None:
            raw_price = _find_col(line, "VTE_PRIX_DE_V")
        if raw_price is not None:
            try:
                price = float(raw_price)
            except (ValueError, TypeError):
                pass

        remise = 0.0
        raw_remise = line.get("VTE_REMISE")
        if raw_remise is not None:
            try:
                remise = float(raw_remise)
            except (ValueError, TypeError):
                pass

        effective_price = price * (1 - remise / 100) if remise else price

        agg[art_id]["qty"] += qty
        agg[art_id]["revenue"] += effective_price * qty
        agg[art_id]["price"] = price

        vid = _normalize_id(line.get("VTE_ID"))
        if vid is not None:
            agg[art_id]["txns"].add(vid)

        skip_stats["lines_counted"] += 1

    sales = []
    total_revenue = 0.0
    all_txns = set()

    for art_id, data in agg.items():
        art = articles_map.get(art_id, {})
        rev = round(data["revenue"], 2)
        total_revenue += rev
        all_txns.update(data["txns"])

        sales.append({
            "posArticleId": str(art_id),
            "barcode": art.get("ART_BARCODE") or None,
            "articleName": art.get("ART_ARTICLE", f"ART#{art_id}"),
            "category": _resolve_category(art, categories_map) if art else None,
            "quantitySold": round(data["qty"], 3),
            "weightSoldKg": None,
            "totalRevenue": rev,
            "unitPrice": round(data["price"], 2),
            "transactionCount": len(data["txns"]),
        })

    logger.info(
        "Sales aggregation complete: context=%s articles=%d txns=%d revenue=%.2f skip_stats=%s",
        context_label,
        len(sales),
        len(all_txns),
        round(total_revenue, 2),
        skip_stats,
    )
    logger.info(
        "Sales aggregation sample: context=%s sample=%s",
        context_label,
        [_summarize_sale_item(s) for s in sales[:_SALES_SAMPLE_LIMIT]],
    )

    return {
        "sales": sales,
        "totalRevenue": round(total_revenue, 2),
        "totalTransactions": len(all_txns),
        "skipStats": skip_stats,
    }


def _read_archive_details(vd_path: str) -> list[dict]:
    """Read detail lines from a daily archive VD file."""
    try:
        rows = read_table(vd_path)
        logger.info("Archive read: path=%s detail_lines=%d", vd_path, len(rows))
        return rows
    except Exception as e:
        logger.warning("Error reading archive %s: %s", vd_path, e)
        return []


def _read_live_details(from_dt: datetime, to_dt: datetime, tmp_dir: str) -> list[dict]:
    """
    Read detail lines from live NOTE_ENTETE + NOTE_DETAIL tables.
    Filters by date range and VTE_CACHE on receipt headers.
    """
    ne_path = os.path.join(tmp_dir, "NOTE_ENTETE.DB")
    nd_path = os.path.join(tmp_dir, "NOTE_DETAIL.DB")

    if not os.path.isfile(ne_path) or not os.path.isfile(nd_path):
        logger.warning("Live tables not found in %s", tmp_dir)
        return []

    entetes = read_table(ne_path)
    details = read_table(nd_path)

    date_col = _find_col_name(entetes, _DATE_COL_PREFIX)
    if not date_col:
        date_col = _find_col_name(entetes, "VTE_DATE")

    valid_vte_ids = set()
    skipped_receipts_cache = 0
    for row in entetes:
        vc = row.get("VTE_CACHE")
        if vc is not None:
            try:
                if int(float(vc)) != 0:
                    skipped_receipts_cache += 1
                    continue
            except (ValueError, TypeError):
                pass

        rec_date = row.get(date_col) if date_col else None
        if is_in_period(rec_date, from_dt, to_dt):
            vte_id = _normalize_id(row.get("VTE_ID"))
            if vte_id is not None:
                valid_vte_ids.add(vte_id)

    filtered = []
    for line in details:
        vid = _normalize_id(line.get("VTE_ID"))
        if vid in valid_vte_ids:
            filtered.append(line)

    logger.info(
        "Live read complete: from=%s to=%s date_col=%s entetes=%d skipped_receipts_cache=%d valid_receipts=%d detail_lines_total=%d detail_lines_filtered=%d",
        from_dt.isoformat(),
        to_dt.isoformat(),
        date_col,
        len(entetes),
        skipped_receipts_cache,
        len(valid_vte_ids),
        len(details),
        len(filtered),
    )
    return filtered


def get_sales(from_dt: datetime, to_dt: datetime) -> dict:
    """
    Get aggregated sales per product for a given period.
    """
    logger.info(
        "Sales service fetch start: from=%s to=%s",
        from_dt.isoformat(),
        to_dt.isoformat(),
    )

    tmp_dir = safe_copy_tables(["ARTICLES", "CLASSIFICATION"])
    live_tmp_dir = None
    try:
        articles_map = _build_articles_map(tmp_dir)
        categories_map = _build_category_map(tmp_dir)

        all_details = []
        sources = set()

        current = from_dt.date() if hasattr(from_dt, "date") else from_dt
        end = to_dt.date() if hasattr(to_dt, "date") else to_dt

        dates_needing_live = []
        while current <= end:
            vd_path, ve_path = get_archive_paths(current)
            logger.info(
                "Sales service checking date=%s archive_vd=%s archive_ve=%s has_vd=%s has_ve=%s",
                current.isoformat(),
                vd_path,
                ve_path,
                bool(vd_path),
                bool(ve_path),
            )

            if vd_path:
                archive_details = _read_archive_details(vd_path)
                all_details.extend(archive_details)
                sources.add("archive")
                logger.info(
                    "Sales service using archive for date=%s path=%s detail_lines=%d",
                    current.isoformat(),
                    vd_path,
                    len(archive_details),
                )
            else:
                dates_needing_live.append(current)
                logger.info(
                    "Sales service falling back to live tables for date=%s",
                    current.isoformat(),
                )

            current += timedelta(days=1)

        if dates_needing_live:
            live_tmp_dir = safe_copy_tables(["NOTE_ENTETE", "NOTE_DETAIL"])
            for live_date in dates_needing_live:
                day_start = localize_naive(datetime(live_date.year, live_date.month, live_date.day, 0, 0, 0))
                day_end = localize_naive(datetime(live_date.year, live_date.month, live_date.day, 23, 59, 59))
                logger.info(
                    "Sales service fetching live details for date=%s from=%s to=%s",
                    live_date.isoformat(),
                    day_start.isoformat(),
                    day_end.isoformat(),
                )
                live_details = _read_live_details(day_start, day_end, live_tmp_dir)
                if live_details:
                    all_details.extend(live_details)
                    sources.add("live")
                logger.info(
                    "Sales service live fetch complete for date=%s detail_lines=%d",
                    live_date.isoformat(),
                    len(live_details),
                )

        if sources == {"archive", "live"}:
            source = "archive+live"
        elif sources == {"archive"}:
            source = "archive"
        elif sources == {"live"}:
            source = "live"
        else:
            source = "none"

        logger.info(
            "Sales service collected detail lines: from=%s to=%s source=%s total_detail_lines=%d",
            from_dt.isoformat(),
            to_dt.isoformat(),
            source,
            len(all_details),
        )

        result = _aggregate_details(
            all_details,
            articles_map,
            categories_map,
            context_label=f"{from_dt.isoformat()} -> {to_dt.isoformat()}",
        )

        now = datetime.now().astimezone()
        response = {
            "sales": result["sales"],
            "metadata": {
                "periodFrom": from_dt.isoformat(),
                "periodTo": to_dt.isoformat(),
                "totalTransactions": result["totalTransactions"],
                "totalRevenue": result["totalRevenue"],
                "generatedAt": now.isoformat(),
                "source": source,
            },
        }

        logger.info(
            "Sales service fetch complete: from=%s to=%s source=%s sales=%d txns=%d revenue=%.2f",
            from_dt.isoformat(),
            to_dt.isoformat(),
            source,
            len(response["sales"]),
            response["metadata"]["totalTransactions"],
            response["metadata"]["totalRevenue"],
        )
        return response
    finally:
        cleanup_temp(tmp_dir)
        if live_tmp_dir:
            cleanup_temp(live_tmp_dir)
