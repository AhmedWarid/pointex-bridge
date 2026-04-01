import argparse
from datetime import date, datetime
from pathlib import Path

from app.config import settings
from app.services.paradox_reader import read_table


LIVE_TABLES = ("NOTE_DETAIL.DB", "NOTE_DETAIL_RESA.DB")


def normalize_id(value):
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError, OverflowError):
        return None


def parse_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_date_only(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def sample_row(row):
    keys = [
        "ART_ID",
        "VTE_ID",
        "VTE_QUANTITE",
        "VTE_TYPE_LIGNE",
        "VTE_CACHE",
        "VTE_PRIX_DE_VENTE",
        "VTE_REMISE",
        "VTE_HEURE",
        "VTE_DATE_DE_LA_PIECE",
        "VTE_DATE_DE_CLOTURE",
        "VTE_CLOTUREE",
    ]
    result = {}
    for key in keys:
        if key in row:
            result[key] = row[key]
    return result


def build_receipt_lookup(entete_rows):
    lookup = {}
    for row in entete_rows:
        vte_id = normalize_id(row.get("VTE_ID"))
        if vte_id is None:
            continue
        lookup[vte_id] = {
            "VTE_DATE_DE_LA_PIECE": parse_date_only(row.get("VTE_DATE_DE_LA_PIECE")),
            "VTE_DATE_DE_CLOTURE": parse_date_only(row.get("VTE_DATE_DE_CLOTURE")),
            "VTE_CLOTUREE": row.get("VTE_CLOTUREE"),
            "VTE_CACHE": row.get("VTE_CACHE"),
        }
    return lookup


def load_note_entete(base_path: Path):
    entete_path = base_path / "NOTE_ENTETE.DB"
    if not entete_path.exists():
        return {}
    return build_receipt_lookup(read_table(str(entete_path)))


def table_matches_for_day(table_path: Path, article_id: int, target_day: date, receipt_lookup: dict):
    rows = read_table(str(table_path))
    matches = []

    for row in rows:
        if normalize_id(row.get("ART_ID")) != article_id:
            continue

        vte_id = normalize_id(row.get("VTE_ID"))
        parent = receipt_lookup.get(vte_id, {})
        piece_day = parse_date_only(row.get("VTE_DATE_DE_LA_PIECE")) or parent.get("VTE_DATE_DE_LA_PIECE")
        cloture_day = parse_date_only(row.get("VTE_DATE_DE_CLOTURE")) or parent.get("VTE_DATE_DE_CLOTURE")

        if piece_day != target_day and cloture_day != target_day:
            continue

        qty = parse_float(row.get("VTE_QUANTITE")) or 0.0
        payload = sample_row(row)
        if parent:
            payload["PARENT_VTE_DATE_DE_LA_PIECE"] = parent.get("VTE_DATE_DE_LA_PIECE")
            payload["PARENT_VTE_DATE_DE_CLOTURE"] = parent.get("VTE_DATE_DE_CLOTURE")
            payload["PARENT_VTE_CLOTUREE"] = parent.get("VTE_CLOTUREE")
            payload["PARENT_VTE_CACHE"] = parent.get("VTE_CACHE")
        payload["_SORT_HEURE"] = row.get("VTE_HEURE")
        payload["_QTY"] = qty
        matches.append(payload)

    matches.sort(key=lambda row: (str(row.get("_SORT_HEURE") or ""), normalize_id(row.get("VTE_ID")) or 0))
    return matches


def summarize_rows(rows):
    qty_sum = sum(float(row.get("_QTY") or 0.0) for row in rows)
    unique_vte_ids = {
        normalize_id(row.get("VTE_ID"))
        for row in rows
        if normalize_id(row.get("VTE_ID")) is not None
    }
    return {
        "rows": len(rows),
        "qty_sum": qty_sum,
        "unique_vte_ids": len(unique_vte_ids),
    }


def print_table_result(table_name: str, rows):
    summary = summarize_rows(rows)
    print(table_name)
    print(
        f"  rows={summary['rows']} qty_sum={summary['qty_sum']:.3f} "
        f"unique_vte_ids={summary['unique_vte_ids']}"
    )
    if not rows:
        print("  no current-day rows")
        print()
        return
    for row in rows:
        row = dict(row)
        row.pop("_SORT_HEURE", None)
        row.pop("_QTY", None)
        print(f"  row={row}")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Compare current-day live rows for one ART_ID in NOTE_DETAIL and NOTE_DETAIL_RESA."
    )
    parser.add_argument("article_id", type=int, help="Target ART_ID, e.g. 295")
    parser.add_argument(
        "--path",
        default=settings.saveurs_path,
        help="Base SAVEURS path. Defaults to configured SAVEURS_PATH.",
    )
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Business day in YYYY-MM-DD. Defaults to today.",
    )
    args = parser.parse_args()

    base_path = Path(args.path)
    if not base_path.exists():
        raise SystemExit(f"Path does not exist: {base_path}")

    try:
        target_day = date.fromisoformat(args.date)
    except ValueError as exc:
        raise SystemExit(f"Invalid --date value: {args.date}") from exc

    print(f"ART_ID={args.article_id}")
    print(f"Base path: {base_path}")
    print(f"Business day: {target_day.isoformat()}")
    print()

    receipt_lookup = load_note_entete(base_path)

    for table_name in LIVE_TABLES:
        table_path = base_path / table_name
        if not table_path.exists():
            print(table_name)
            print("  missing")
            print()
            continue
        rows = table_matches_for_day(table_path, args.article_id, target_day, receipt_lookup)
        print_table_result(table_name, rows)


if __name__ == "__main__":
    main()
