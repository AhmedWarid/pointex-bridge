import argparse
import os
from pathlib import Path

from app.config import settings
from app.services.paradox_reader import read_table


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


def iter_db_files(base_path: Path, recursive: bool):
    if recursive:
        yield from sorted(base_path.rglob("*.DB"))
    else:
        yield from sorted(base_path.glob("*.DB"))


def sample_row(row: dict):
    interesting_keys = [
        "ART_ID",
        "VTE_ID",
        "VTE_QUANTITE",
        "VTE_TYPE_LIGNE",
        "VTE_CACHE",
        "VTE_PRIX_DE_VENTE",
        "VTE_REMISE",
        "VTE_DATE_DE_LA_PIECE",
        "VTE_DATE_DE_CLOTURE",
        "VTE_CLOTUREE",
        "VTE_HEURE",
        "ART_ARTICLE",
        "ART_CODE",
        "ART_BARCODE",
    ]
    result = {}
    for key in interesting_keys:
        if key in row:
            result[key] = row[key]
    if not result:
        for key, value in row.items():
            result[key] = value
            if len(result) >= 8:
                break
    return result


def scan_table(table_path: Path, article_id: int, sample_limit: int):
    try:
        rows = read_table(str(table_path))
    except Exception as exc:
        return {"table": str(table_path), "error": str(exc)}

    matches = []
    qty_sum = 0.0
    qty_rows = 0
    vte_ids = set()

    for row in rows:
        if normalize_id(row.get("ART_ID")) != article_id:
            continue
        matches.append(row)
        qty = parse_float(row.get("VTE_QUANTITE"))
        if qty is not None:
            qty_sum += qty
            qty_rows += 1
        vte_id = normalize_id(row.get("VTE_ID"))
        if vte_id is not None:
            vte_ids.add(vte_id)

    if not matches:
        return None

    return {
        "table": str(table_path),
        "match_rows": len(matches),
        "qty_sum": qty_sum,
        "qty_rows": qty_rows,
        "unique_vte_ids": len(vte_ids),
        "samples": [sample_row(row) for row in matches[:sample_limit]],
    }


def main():
    parser = argparse.ArgumentParser(
        description="Brute-force scan Paradox tables for rows matching a specific ART_ID."
    )
    parser.add_argument("article_id", type=int, help="Target ART_ID to search for, e.g. 295")
    parser.add_argument(
        "--path",
        default=settings.saveurs_path,
        help="Base SAVEURS path to scan. Defaults to configured SAVEURS_PATH.",
    )
    parser.add_argument(
        "--top-level-only",
        action="store_true",
        help="Only scan .DB files directly under SAVEURS. Default scans all subfolders recursively.",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=5,
        help="How many matching rows to print per table.",
    )
    args = parser.parse_args()

    base_path = Path(args.path)
    if not base_path.exists():
        raise SystemExit(f"Path does not exist: {base_path}")

    print(f"Scanning for ART_ID={args.article_id}")
    print(f"Base path: {base_path}")
    print(f"Recursive: {not args.top_level_only}")
    print()

    results = []
    errors = []

    for table_path in iter_db_files(base_path, not args.top_level_only):
        result = scan_table(table_path, args.article_id, args.sample_limit)
        if not result:
            continue
        if "error" in result:
            errors.append(result)
            continue
        results.append(result)

    results.sort(
        key=lambda item: (
            item["qty_sum"],
            item["match_rows"],
            item["unique_vte_ids"],
            item["table"],
        ),
        reverse=True,
    )

    if not results:
        print("No matching rows found.")
    else:
        print(f"Tables with matches: {len(results)}")
        print()
        for item in results:
            print(item["table"])
            print(
                f"  rows={item['match_rows']} qty_sum={item['qty_sum']:.3f} "
                f"qty_rows={item['qty_rows']} unique_vte_ids={item['unique_vte_ids']}"
            )
            for sample in item["samples"]:
                print(f"  sample={sample}")
            print()

    if errors:
        print("Tables with read errors:")
        for item in errors:
            print(f"  {item['table']}: {item['error']}")


if __name__ == "__main__":
    main()
