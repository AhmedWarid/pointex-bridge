"""
Pointex Bridge — Interactive Testing Tool
==========================================
Run with:  python test_bridge.py

Tests connection to SAVEURS path, reads Paradox tables,
lets you query data interactively, and export results.
"""

import csv
import json
import os
import sys
import tempfile
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path so we can import app modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.config import settings
from app.services.paradox_reader import read_table
from app.services.file_manager import PARADOX_EXTENSIONS

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BLUE = "\033[94m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"
DIM = "\033[2m"


def cprint(color, text):
    print(f"{color}{text}{RESET}")


def separator(char="─", width=60):
    print(f"{DIM}{char * width}{RESET}")


def safe_copy_single(table_name: str, saveurs_path: str) -> str | None:
    """Copy one table + companions to temp dir. Returns temp dir path or None."""
    tmp_dir = tempfile.mkdtemp(prefix="pointex_test_")
    found_db = False
    for ext in PARADOX_EXTENSIONS:
        src = os.path.join(saveurs_path, f"{table_name}{ext}")
        if os.path.exists(src):
            if ext == ".DB":
                found_db = True
            for attempt in range(3):
                try:
                    shutil.copy2(src, tmp_dir)
                    break
                except PermissionError:
                    if attempt == 2:
                        shutil.rmtree(tmp_dir, ignore_errors=True)
                        return None
                    time.sleep(0.5)
    if not found_db:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return None
    return tmp_dir


def cleanup(tmp_dir: str):
    shutil.rmtree(tmp_dir, ignore_errors=True)


def export_rows(rows: list[dict], filename: str):
    """Export rows to CSV, JSON-lines TXT, or XLSX."""
    ext = Path(filename).suffix.lower()

    if not rows:
        cprint(YELLOW, "  No data to export.")
        return

    if ext == ".csv":
        keys = list(rows[0].keys())
        with open(filename, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                clean = {}
                for k in keys:
                    v = row.get(k)
                    if isinstance(v, datetime):
                        clean[k] = v.isoformat()
                    elif isinstance(v, bytes):
                        clean[k] = v.hex()
                    else:
                        clean[k] = v
                writer.writerow(clean)
        cprint(GREEN, f"  Exported {len(rows)} rows to {filename}")

    elif ext == ".txt":
        with open(filename, "w", encoding="utf-8") as f:
            for row in rows:
                clean = {}
                for k, v in row.items():
                    if isinstance(v, datetime):
                        clean[k] = v.isoformat()
                    elif isinstance(v, bytes):
                        clean[k] = v.hex()
                    else:
                        clean[k] = v
                f.write(json.dumps(clean, ensure_ascii=False, default=str) + "\n")
        cprint(GREEN, f"  Exported {len(rows)} rows to {filename}")

    elif ext == ".xlsx":
        try:
            import openpyxl
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = "Data"
            keys = list(rows[0].keys())
            ws.append(keys)
            for row in rows:
                vals = []
                for k in keys:
                    v = row.get(k)
                    if isinstance(v, datetime):
                        vals.append(v.isoformat())
                    elif isinstance(v, bytes):
                        vals.append(v.hex())
                    elif isinstance(v, set):
                        vals.append(str(v))
                    else:
                        vals.append(v)
                ws.append(vals)
            wb.save(filename)
            cprint(GREEN, f"  Exported {len(rows)} rows to {filename}")
        except ImportError:
            cprint(RED, "  openpyxl not installed. Run: pip install openpyxl")
            alt = filename.replace(".xlsx", ".csv")
            cprint(YELLOW, f"  Falling back to CSV: {alt}")
            export_rows(rows, alt)
    else:
        cprint(RED, f"  Unknown format: {ext}. Use .csv, .txt, or .xlsx")


# ---------------------------------------------------------------------------
# Test 1: Connection
# ---------------------------------------------------------------------------

def test_connection():
    separator("=")
    cprint(BOLD, " TEST 1: Connection to SAVEURS path")
    separator("=")
    path = settings.saveurs_path
    print(f"  Path: {path}")

    if os.path.isdir(path):
        cprint(GREEN, "  Status: ACCESSIBLE")
    else:
        cprint(RED, "  Status: NOT ACCESSIBLE")
        cprint(YELLOW, "  Make sure CAISSE-PC is on and the share is reachable.")
        cprint(YELLOW, f"  Try opening {path} in Windows Explorer.")
        return False
    return True


# ---------------------------------------------------------------------------
# Test 2: Discover all .DB files
# ---------------------------------------------------------------------------

def discover_tables(saveurs_path: str) -> list[str]:
    separator("=")
    cprint(BOLD, " TEST 2: Discovering Paradox tables")
    separator("=")

    db_files = sorted([
        f[:-3]  # strip .DB
        for f in os.listdir(saveurs_path)
        if f.upper().endswith(".DB")
    ])

    print(f"  Found {len(db_files)} .DB files:\n")

    for i, name in enumerate(db_files, 1):
        companions = []
        for ext in [".PX", ".MB", ".XG0", ".YG0", ".VAL"]:
            if os.path.exists(os.path.join(saveurs_path, f"{name}{ext}")):
                companions.append(ext)

        db_path = os.path.join(saveurs_path, f"{name}.DB")
        size_kb = os.path.getsize(db_path) / 1024
        comp_str = " ".join(companions) if companions else "(no companions)"
        print(f"  {i:3}. {CYAN}{name:30s}{RESET} {size_kb:8.1f} KB  {DIM}{comp_str}{RESET}")

    return db_files


# ---------------------------------------------------------------------------
# Test 3: Read each table
# ---------------------------------------------------------------------------

def test_read_tables(tables: list[str], saveurs_path: str) -> dict[str, list[dict]]:
    separator("=")
    cprint(BOLD, " TEST 3: Reading tables")
    separator("=")

    results = {}
    for name in tables:
        tmp_dir = safe_copy_single(name, saveurs_path)
        if tmp_dir is None:
            cprint(RED, f"  {name:30s}  FAILED (copy error or missing .DB)")
            continue

        try:
            db_path = os.path.join(tmp_dir, f"{name}.DB")
            rows = read_table(db_path)
            results[name] = rows
            cols = list(rows[0].keys()) if rows else []
            cprint(GREEN, f"  {name:30s}  {len(rows):6d} rows  {len(cols):3d} columns")
        except Exception as e:
            cprint(RED, f"  {name:30s}  ERROR: {e}")
        finally:
            cleanup(tmp_dir)

    return results


# ---------------------------------------------------------------------------
# Test 4: Key tables for ProtoCart
# ---------------------------------------------------------------------------

KEY_TABLES = ["ARTICLES", "VENTE_REGLEE", "ARTICLE_VENDU"]


def test_key_tables(cached: dict[str, list[dict]]):
    separator("=")
    cprint(BOLD, " TEST 4: Key tables for ProtoCart")
    separator("=")

    for table in KEY_TABLES:
        rows = cached.get(table)
        if rows is None:
            cprint(RED, f"  {table}: NOT READ (missing or failed)")
            continue

        print(f"\n  {CYAN}{table}{RESET}  ({len(rows)} rows)")
        if rows:
            cols = list(rows[0].keys())
            print(f"  Columns: {', '.join(cols)}")
            # Show first 3 rows as sample
            print(f"  {DIM}Sample (first 3 rows):{RESET}")
            for row in rows[:3]:
                preview = {}
                for k, v in row.items():
                    if isinstance(v, datetime):
                        preview[k] = v.strftime("%Y-%m-%d %H:%M")
                    elif isinstance(v, bytes):
                        preview[k] = f"<{len(v)}B>"
                    elif isinstance(v, str) and len(v) > 30:
                        preview[k] = v[:30] + "..."
                    else:
                        preview[k] = v
                print(f"    {preview}")


# ---------------------------------------------------------------------------
# Test 5: V2 discovery — find missing lookup tables
# ---------------------------------------------------------------------------

V2_PATTERNS = {
    "Selling price (TARIFS)": ["TARIF", "PRIX", "GRILLE", "PV_"],
    "Categories": ["FAMILLE", "CATEGORIE", "CLASSEMENT", "CLASS"],
    "Units": ["UNITE"],
    "Payment methods": ["REGLEMENT", "MRG", "PAIEMENT", "MODE_REG"],
}


def test_v2_discovery(tables: list[str], cached: dict[str, list[dict]]):
    separator("=")
    cprint(BOLD, " TEST 5: V2 — Discovering lookup tables")
    separator("=")

    for feature, patterns in V2_PATTERNS.items():
        matches = [
            t for t in tables
            if any(p.upper() in t.upper() for p in patterns)
        ]
        if matches:
            cprint(GREEN, f"  {feature}:")
            for m in matches:
                rows = cached.get(m, [])
                cols = list(rows[0].keys()) if rows else ["?"]
                print(f"    {CYAN}{m}{RESET}  ({len(rows)} rows)  Columns: {', '.join(cols)}")
        else:
            cprint(YELLOW, f"  {feature}: No matching table found")

    # Check custom fields in ARTICLES
    art_rows = cached.get("ARTICLES", [])
    if art_rows:
        print()
        cprint(BOLD, "  Checking ARTICLES custom fields for selling price:")
        sample = art_rows[0]
        for field in ["ART_COEFF_S_V", "ART_LIBRE_NUM_1", "ART_LIBRE_NUM_2",
                       "ART_LIBRE_NUM_3", "ART_LIBRE_NUM_4", "ART_LIBRE_NUM_5"]:
            # Match by prefix since names may be truncated
            val = None
            matched_key = None
            for k in sample:
                if k.upper().startswith(field.upper()[:12]):
                    val = sample[k]
                    matched_key = k
                    break
            if matched_key:
                # Show a few samples
                samples = [r.get(matched_key) for r in art_rows[:5]]
                non_zero = [s for s in samples if s and s != 0 and s != 0.0]
                status = GREEN if non_zero else DIM
                print(f"    {status}{matched_key:25s} samples: {samples}{RESET}")


# ---------------------------------------------------------------------------
# Interactive query mode
# ---------------------------------------------------------------------------

def interactive_mode(tables: list[str], cached: dict[str, list[dict]], saveurs_path: str):
    separator("=")
    cprint(BOLD, " INTERACTIVE QUERY MODE")
    separator("=")
    print(f"""
  Commands:
    {CYAN}list{RESET}                     List all tables
    {CYAN}read <TABLE>{RESET}             Read a table (shows first 20 rows)
    {CYAN}read <TABLE> all{RESET}         Read all rows
    {CYAN}cols <TABLE>{RESET}             Show column names and types
    {CYAN}count <TABLE>{RESET}            Show row count
    {CYAN}search <TABLE> <COL> <VAL>{RESET}  Filter rows where COL contains VAL
    {CYAN}sales <from> <to>{RESET}        Test sales query (dates as YYYY-MM-DD)
    {CYAN}sales today{RESET}              Sales for today (04:00 → 23:59)
    {CYAN}export <TABLE> <file>{RESET}    Export to .csv / .txt / .xlsx
    {CYAN}exportq <file>{RESET}           Export last query result
    {CYAN}quit{RESET}                     Exit
""")

    last_result: list[dict] = []

    while True:
        try:
            raw = input(f"{BOLD}bridge>{RESET} ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not raw:
            continue

        parts = raw.split()
        cmd = parts[0].lower()

        # ---- list ----
        if cmd == "list":
            for i, t in enumerate(tables, 1):
                rows = cached.get(t)
                count = len(rows) if rows is not None else "?"
                print(f"  {i:3}. {t:30s}  {count} rows")

        # ---- read ----
        elif cmd == "read" and len(parts) >= 2:
            tname = parts[1].upper()
            show_all = len(parts) >= 3 and parts[2].lower() == "all"

            if tname not in cached:
                # Try reading it
                tmp = safe_copy_single(tname, saveurs_path)
                if tmp:
                    try:
                        rows = read_table(os.path.join(tmp, f"{tname}.DB"))
                        cached[tname] = rows
                    except Exception as e:
                        cprint(RED, f"  Error reading {tname}: {e}")
                        cleanup(tmp)
                        continue
                    finally:
                        cleanup(tmp)
                else:
                    cprint(RED, f"  Table {tname} not found or not accessible.")
                    continue

            rows = cached[tname]
            last_result = rows
            limit = len(rows) if show_all else min(20, len(rows))
            print(f"  {tname}: {len(rows)} rows (showing {limit})\n")

            if rows:
                cols = list(rows[0].keys())
                # Print header
                header = " | ".join(f"{c[:18]:18s}" for c in cols[:8])
                if len(cols) > 8:
                    header += f" ... +{len(cols)-8} more"
                print(f"  {DIM}{header}{RESET}")
                separator("─", len(header) + 4)

                for row in rows[:limit]:
                    vals = []
                    for c in cols[:8]:
                        v = row.get(c)
                        if isinstance(v, datetime):
                            vals.append(v.strftime("%Y-%m-%d %H:%M"))
                        elif isinstance(v, float):
                            vals.append(f"{v:.2f}")
                        elif isinstance(v, bytes):
                            vals.append(f"<{len(v)}B>")
                        elif v is None:
                            vals.append("")
                        else:
                            vals.append(str(v)[:18])
                    print(f"  {' | '.join(f'{v:18s}' for v in vals)}")

        # ---- cols ----
        elif cmd == "cols" and len(parts) >= 2:
            tname = parts[1].upper()
            rows = cached.get(tname, [])
            if not rows:
                cprint(YELLOW, f"  {tname} not loaded. Use 'read {tname}' first.")
                continue
            sample = rows[0]
            print(f"  {tname}: {len(sample)} columns\n")
            for i, (k, v) in enumerate(sample.items(), 1):
                vtype = type(v).__name__
                sample_val = v
                if isinstance(v, datetime):
                    sample_val = v.strftime("%Y-%m-%d %H:%M")
                elif isinstance(v, bytes):
                    sample_val = f"<{len(v)} bytes>"
                elif isinstance(v, str) and len(v) > 40:
                    sample_val = v[:40] + "..."
                print(f"  {i:3}. {k:30s}  {vtype:10s}  sample: {sample_val}")

        # ---- count ----
        elif cmd == "count" and len(parts) >= 2:
            tname = parts[1].upper()
            rows = cached.get(tname)
            if rows is not None:
                print(f"  {tname}: {len(rows)} rows")
            else:
                cprint(YELLOW, f"  {tname} not loaded. Use 'read {tname}' first.")

        # ---- search ----
        elif cmd == "search" and len(parts) >= 4:
            tname = parts[1].upper()
            col_search = parts[2].upper()
            val_search = " ".join(parts[3:]).upper()

            rows = cached.get(tname, [])
            if not rows:
                cprint(YELLOW, f"  {tname} not loaded. Use 'read {tname}' first.")
                continue

            # Find matching column
            matched_col = None
            for k in rows[0]:
                if k.upper().startswith(col_search):
                    matched_col = k
                    break
            if not matched_col:
                cprint(RED, f"  Column starting with '{col_search}' not found.")
                continue

            matches = []
            for row in rows:
                v = row.get(matched_col)
                if v is not None and val_search in str(v).upper():
                    matches.append(row)

            last_result = matches
            print(f"  Found {len(matches)} rows where {matched_col} contains '{val_search}'")
            for row in matches[:20]:
                preview = {k: (str(v)[:25] if v is not None else "") for k, v in row.items()}
                print(f"    {preview}")

        # ---- sales ----
        elif cmd == "sales":
            from app.utils.date_utils import parse_iso, get_tz
            from app.services.sales_service import get_sales
            from zoneinfo import ZoneInfo

            if len(parts) >= 3:
                from_str = parts[1]
                to_str = parts[2]
            elif len(parts) == 2 and parts[1].lower() == "today":
                from zoneinfo import ZoneInfo
                tz = get_tz()
                today = datetime.now(tz).date()
                from_str = f"{today}T04:00:00"
                to_str = f"{today}T23:59:59"
            else:
                cprint(YELLOW, "  Usage: sales <from> <to>  or  sales today")
                cprint(YELLOW, "  Dates as YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS")
                continue

            try:
                # Add time if only date given
                if "T" not in from_str:
                    from_str += "T04:00:00"
                if "T" not in to_str:
                    to_str += "T23:59:59"

                from_dt = parse_iso(from_str)
                to_dt = parse_iso(to_str)

                cprint(DIM, f"  Querying sales from {from_dt.isoformat()} to {to_dt.isoformat()}...")
                result = get_sales(from_dt, to_dt)

                sales = result["sales"]
                meta = result["metadata"]
                last_result = sales

                print()
                cprint(BOLD, f"  Sales Results: {len(sales)} articles sold")
                separator()
                print(f"  Total transactions: {meta['totalTransactions']}")
                print(f"  Total revenue:      {meta['totalRevenue']:.2f} DH")
                print(f"  Period:             {meta['periodFrom']} → {meta['periodTo']}")
                print()

                if sales:
                    print(f"  {'Article':30s} {'Qty':>8s} {'Revenue':>10s} {'UnitPrice':>10s} {'Txns':>6s}")
                    separator()
                    for s in sorted(sales, key=lambda x: x["totalRevenue"], reverse=True):
                        name = (s["articleName"] or "?")[:30]
                        print(f"  {name:30s} {s['quantitySold']:8.1f} {s['totalRevenue']:10.2f} {s['unitPrice']:10.2f} {s['transactionCount']:6d}")

            except Exception as e:
                cprint(RED, f"  Error: {e}")

        # ---- export ----
        elif cmd == "export" and len(parts) >= 3:
            tname = parts[1].upper()
            filename = parts[2]
            rows = cached.get(tname, [])
            if not rows:
                cprint(YELLOW, f"  {tname} not loaded. Use 'read {tname}' first.")
                continue
            export_rows(rows, filename)

        # ---- exportq ----
        elif cmd == "exportq" and len(parts) >= 2:
            filename = parts[1]
            if not last_result:
                cprint(YELLOW, "  No query results to export. Run a query first.")
                continue
            export_rows(last_result, filename)

        # ---- quit ----
        elif cmd in ("quit", "exit", "q"):
            break

        else:
            cprint(YELLOW, f"  Unknown command: {raw}")
            cprint(YELLOW, "  Type a command or 'quit' to exit.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    os.system("")  # Enable ANSI colors on Windows

    print()
    cprint(BOLD + CYAN, "  ╔══════════════════════════════════════════╗")
    cprint(BOLD + CYAN, "  ║   Pointex Bridge — Interactive Tester   ║")
    cprint(BOLD + CYAN, "  ╚══════════════════════════════════════════╝")
    print()

    saveurs_path = settings.saveurs_path
    print(f"  Config loaded from .env")
    print(f"  SAVEURS_PATH = {saveurs_path}")
    print(f"  TIMEZONE     = {settings.timezone}")
    print()

    # Test 1: Connection
    if not test_connection():
        cprint(RED, "\n  Cannot proceed without access to SAVEURS path.")
        cprint(YELLOW, "  Fix the path in .env or ensure CAISSE-PC is reachable.")
        input("\n  Press Enter to exit...")
        return

    # Test 2: Discover tables
    tables = discover_tables(saveurs_path)
    if not tables:
        cprint(RED, "\n  No .DB files found!")
        input("\n  Press Enter to exit...")
        return

    print()
    cprint(BOLD, "  Reading all tables? This scans every .DB file to verify readability.")
    choice = input(f"  Read all {len(tables)} tables? [Y/n] ").strip().lower()
    if choice in ("", "y", "yes"):
        # Test 3: Read all tables
        cached = test_read_tables(tables, saveurs_path)

        print()
        # Test 4: Key tables
        test_key_tables(cached)

        print()
        # Test 5: V2 discovery
        test_v2_discovery(tables, cached)
    else:
        # Only read key tables
        cprint(DIM, "  Skipping full scan. Reading key tables only...")
        cached = test_read_tables(KEY_TABLES, saveurs_path)

    print()
    # Interactive mode
    interactive_mode(tables, cached, saveurs_path)

    print()
    cprint(GREEN, "  Done. Goodbye!")


if __name__ == "__main__":
    main()
