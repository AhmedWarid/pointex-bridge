"""
Pointex Bridge — Interactive Testing Tool
==========================================
Run with:  python test_bridge.py

Tests connection to SAVEURS path, reads Paradox tables,
lets you query data interactively, and export results.

All output is also saved to test_report.log for debugging.
"""

import csv
import io
import json
import os
import platform
import socket
import struct
import subprocess
import sys
import tempfile
import shutil
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path so we can import app modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ---------------------------------------------------------------------------
# Dual output — print to console AND log file simultaneously
# ---------------------------------------------------------------------------

LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_report.log")

class TeeWriter:
    """Writes to both the real stdout and a log file."""
    def __init__(self, log_path: str):
        self._stdout = sys.stdout
        self._log = open(log_path, "w", encoding="utf-8")
    def write(self, text):
        # Strip ANSI codes for the log file
        import re
        clean = re.sub(r"\033\[[0-9;]*m", "", text)
        self._log.write(clean)
        self._log.flush()
        self._stdout.write(text)
        self._stdout.flush()
    def flush(self):
        self._log.flush()
        self._stdout.flush()
    def close(self):
        self._log.close()

_tee = TeeWriter(LOG_FILE)
sys.stdout = _tee

# ---------------------------------------------------------------------------
# Colors
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


def print_error(context: str, exc: Exception):
    """Print a detailed error block with full traceback."""
    cprint(RED, f"\n  ERROR in {context}:")
    cprint(RED, f"  Type:    {type(exc).__name__}")
    cprint(RED, f"  Message: {exc}")
    # Full traceback
    tb = traceback.format_exception(type(exc), exc, exc.__traceback__)
    cprint(DIM, "  --- Full Traceback ---")
    for line in tb:
        for subline in line.rstrip().split("\n"):
            cprint(DIM, f"  {subline}")
    cprint(DIM, "  --- End Traceback ---\n")


# ---------------------------------------------------------------------------
# File helpers
# ---------------------------------------------------------------------------

def _noop():
    pass


def safe_copy_single(table_name: str, saveurs_path: str) -> tuple[str | None, str | None]:
    """
    Copy one Paradox table + ALL companion files to temp dir.
    Finds all files matching the table name prefix (e.g. ARTICLES.DB,
    ARTICLES.PX, ARTICLES.MB, ARTICLES.XG0, ARTICLES.XG1, etc.)
    Returns (temp_dir_path, error_message). One will be None.
    """
    tmp_dir = tempfile.mkdtemp(prefix="pointex_test_")

    # Find ALL companion files by prefix
    prefix = table_name.upper() + "."
    try:
        all_files = [f for f in os.listdir(saveurs_path) if f.upper().startswith(prefix)]
    except Exception as e:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return None, f"Cannot list directory {saveurs_path}: {e}"

    if not all_files:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return None, f"No files found for {table_name} in {saveurs_path}"

    found_db = any(f.upper().endswith(".DB") for f in all_files)
    if not found_db:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return None, f"{table_name}.DB not found in {saveurs_path}"

    copied_files = []
    errors = []
    for fname in all_files:
        src = os.path.join(saveurs_path, fname)
        last_err = None
        for attempt in range(3):
            try:
                shutil.copy2(src, os.path.join(tmp_dir, fname))
                copied_files.append(fname)
                break
            except PermissionError as e:
                last_err = e
                if attempt < 2:
                    time.sleep(0.5)
            except Exception as e:
                last_err = e
                break

        if last_err and fname.upper().endswith(".DB"):
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return None, f"Failed to copy {fname}: {type(last_err).__name__}: {last_err}"
        elif last_err:
            errors.append(f"{fname}: {type(last_err).__name__}: {last_err}")

    # Verify copied .DB file is not empty
    copied_db = os.path.join(tmp_dir, f"{table_name}.DB")
    if not os.path.exists(copied_db):
        # Try case-insensitive match
        for f in os.listdir(tmp_dir):
            if f.upper() == f"{table_name}.DB".upper():
                copied_db = os.path.join(tmp_dir, f)
                break

    copied_size = os.path.getsize(copied_db)
    src_db = os.path.join(saveurs_path, f"{table_name}.DB")
    src_size = os.path.getsize(src_db)

    cprint(DIM, f"    Copied {len(copied_files)} files: {', '.join(copied_files)}")

    if copied_size == 0 and src_size > 0:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return None, (
            f"{table_name}.DB copied as 0 bytes (source is {src_size} bytes). "
            f"The file is likely locked by the POS. Try when POS is not running."
        )

    return tmp_dir, None


def cleanup(tmp_dir: str):
    shutil.rmtree(tmp_dir, ignore_errors=True)


def export_rows(rows: list[dict], filename: str):
    """Export rows to CSV, JSON-lines TXT, or XLSX."""
    # If filename has no directory, write next to the script
    if not os.path.dirname(filename):
        filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
    ext = Path(filename).suffix.lower()

    if not rows:
        cprint(YELLOW, "  No data to export.")
        return

    try:
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
                        elif isinstance(v, set):
                            clean[k] = str(v)
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
                        elif isinstance(v, set):
                            clean[k] = str(v)
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
    except Exception as e:
        print_error(f"exporting to {filename}", e)


# ---------------------------------------------------------------------------
# Test 0: Environment & prerequisites
# ---------------------------------------------------------------------------

def test_environment():
    separator("=")
    cprint(BOLD, " TEST 0: Environment Check")
    separator("=")

    print(f"  Python:      {sys.version}")
    print(f"  Platform:    {platform.platform()}")
    print(f"  Machine:     {platform.node()}")
    print(f"  CWD:         {os.getcwd()}")
    print(f"  Script dir:  {os.path.dirname(os.path.abspath(__file__))}")
    print()

    # Check .env exists
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        cprint(GREEN, f"  .env file:   Found at {env_path}")
    else:
        cprint(RED, f"  .env file:   NOT FOUND at {env_path}")
        cprint(YELLOW, "  Copy .env.example to .env and edit it!")
        return False

    # Try loading settings
    try:
        from app.config import settings
        cprint(GREEN, f"  Settings loaded OK")
        print(f"  SAVEURS_PATH = {settings.saveurs_path}")
        print(f"  API_PORT     = {settings.api_port}")
        print(f"  TIMEZONE     = {settings.timezone}")
        print(f"  API_KEY      = {'***' + settings.api_key[-4:] if len(settings.api_key) > 4 else '(too short!)'}")
    except Exception as e:
        print_error("loading settings from .env", e)
        return False

    # Check key imports
    print()
    imports_ok = True
    for module_name, desc in [
        ("fastapi", "FastAPI framework"),
        ("uvicorn", "ASGI server"),
        ("pydantic", "Data validation"),
        ("pydantic_settings", "Settings management"),
    ]:
        try:
            mod = __import__(module_name)
            version = getattr(mod, "__version__", "?")
            cprint(GREEN, f"  {desc:25s} {module_name} {version}")
        except ImportError as e:
            cprint(RED, f"  {desc:25s} {module_name} — NOT INSTALLED")
            cprint(RED, f"    Run: pip install {module_name}")
            imports_ok = False

    # Check paradox reader
    print()
    cprint(BOLD, "  Paradox reader availability:")
    reader_found = False
    for lib, desc in [
        ("paradox_reader", "paradox-reader (pure Python)"),
        ("pypxlib", "pypxlib (C wrapper)"),
    ]:
        try:
            __import__(lib)
            cprint(GREEN, f"    {desc}: AVAILABLE")
            reader_found = True
        except ImportError:
            cprint(DIM, f"    {desc}: not installed")

    if reader_found:
        pass
    else:
        cprint(YELLOW, "    No Paradox library installed — will use manual binary parser (built-in fallback)")
        cprint(YELLOW, "    This should work but is less tested. If you see wrong data, try:")
        cprint(YELLOW, "      pip install paradox-reader")

    # Test the paradox reader module itself loads
    try:
        from app.services.paradox_reader import read_table
        cprint(GREEN, f"    paradox_reader.py module: OK")
    except Exception as e:
        print_error("importing paradox_reader module", e)
        return False

    return imports_ok


# ---------------------------------------------------------------------------
# Test 1: Connection
# ---------------------------------------------------------------------------

def test_connection():
    from app.config import settings

    separator("=")
    cprint(BOLD, " TEST 1: Connection to SAVEURS path")
    separator("=")
    path = settings.saveurs_path
    print(f"  Configured path: {path}")

    # Try to parse the hostname from UNC path
    if path.startswith("\\\\"):
        parts = path.lstrip("\\").split("\\")
        hostname = parts[0] if parts else "unknown"
        print(f"  Target host:     {hostname}")

        # Try DNS/ping
        try:
            ip = socket.gethostbyname(hostname)
            cprint(GREEN, f"  DNS resolution:  {hostname} → {ip}")
        except socket.gaierror as e:
            cprint(RED, f"  DNS resolution:  FAILED — {e}")
            cprint(YELLOW, f"  The machine '{hostname}' cannot be found on the network.")
            cprint(YELLOW, f"  Check: Is CAISSE-PC powered on? Is it on the same network?")
            cprint(YELLOW, f"  Try: ping {hostname}")

        # Try ping
        try:
            result = subprocess.run(
                ["ping", "-n", "1", "-w", "2000", hostname],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                cprint(GREEN, f"  Ping:            {hostname} responds")
            else:
                cprint(RED, f"  Ping:            {hostname} NOT responding")
                cprint(DIM, f"    {result.stdout.strip().split(chr(10))[-1] if result.stdout else 'No output'}")
        except Exception as e:
            cprint(YELLOW, f"  Ping:            Could not ping ({e})")

    # Try accessing the directory
    print()
    try:
        exists = os.path.isdir(path)
        if exists:
            cprint(GREEN, f"  Directory access: ACCESSIBLE")
            # List a few files to prove it works
            try:
                files = os.listdir(path)
                db_count = sum(1 for f in files if f.upper().endswith(".DB"))
                print(f"  Files found:      {len(files)} total, {db_count} .DB files")
            except PermissionError as e:
                cprint(RED, f"  Directory listing: PERMISSION DENIED")
                cprint(RED, f"    {e}")
                cprint(YELLOW, "  The path exists but you don't have read permission.")
                return False
            except Exception as e:
                print_error("listing directory contents", e)
                return False
        else:
            cprint(RED, f"  Directory access: NOT ACCESSIBLE")
            # Try to give more specific info
            parent = os.path.dirname(path)
            if os.path.isdir(parent):
                cprint(YELLOW, f"  Parent path {parent} IS accessible.")
                cprint(YELLOW, f"  But '{os.path.basename(path)}' folder not found inside it.")
                try:
                    contents = os.listdir(parent)
                    cprint(YELLOW, f"  Available folders: {', '.join(contents[:10])}")
                except Exception:
                    pass
            else:
                cprint(YELLOW, f"  Cannot reach the network share at all.")
                cprint(YELLOW, f"  Try opening {path} in Windows File Explorer first.")
            return False
    except Exception as e:
        print_error("accessing SAVEURS path", e)
        return False

    return True


# ---------------------------------------------------------------------------
# Test 2: Discover all .DB files
# ---------------------------------------------------------------------------

def discover_tables(saveurs_path: str) -> list[str]:
    separator("=")
    cprint(BOLD, " TEST 2: Discovering Paradox tables")
    separator("=")

    try:
        all_files = os.listdir(saveurs_path)
    except Exception as e:
        print_error("listing SAVEURS directory", e)
        return []

    db_files = sorted([
        f[:-3]  # strip .DB
        for f in all_files
        if f.upper().endswith(".DB")
    ])

    print(f"  Found {len(db_files)} .DB files:\n")

    for i, name in enumerate(db_files, 1):
        companions = []
        for ext in [".PX", ".MB", ".XG0", ".YG0", ".VAL"]:
            if os.path.exists(os.path.join(saveurs_path, f"{name}{ext}")):
                companions.append(ext)

        db_path = os.path.join(saveurs_path, f"{name}.DB")
        try:
            size_kb = os.path.getsize(db_path) / 1024
        except Exception:
            size_kb = 0
        comp_str = " ".join(companions) if companions else "(no companions)"
        print(f"  {i:3}. {CYAN}{name:30s}{RESET} {size_kb:8.1f} KB  {DIM}{comp_str}{RESET}")

    return db_files


# ---------------------------------------------------------------------------
# Test 3: Read each table
# ---------------------------------------------------------------------------

def test_read_tables(tables: list[str], saveurs_path: str) -> dict[str, list[dict]]:
    from app.services.paradox_reader import read_table

    separator("=")
    cprint(BOLD, " TEST 3: Reading tables")
    separator("=")

    results = {}
    errors = {}

    for name in tables:
        tmp_dir, copy_err = safe_copy_single(name, saveurs_path)

        if copy_err:
            cprint(RED, f"  {name:30s}  COPY FAILED")
            cprint(RED, f"    {copy_err}")
            errors[name] = copy_err
            continue

        try:
            db_path = os.path.join(tmp_dir, f"{name}.DB")

            # Log file sizes for debugging
            file_size = os.path.getsize(db_path)
            src_size = os.path.getsize(os.path.join(saveurs_path, f"{name}.DB"))
            size_info = f"({file_size/1024:.1f} KB"
            if file_size != src_size:
                size_info += f", source={src_size/1024:.1f} KB"
            size_info += ")"

            rows = read_table(db_path)
            results[name] = rows
            cols = list(rows[0].keys()) if rows else []

            if rows:
                cprint(GREEN, f"  {name:30s}  {len(rows):6d} rows  {len(cols):3d} columns  {size_info}")
            else:
                cprint(YELLOW, f"  {name:30s}  0 rows (empty table) {size_info}")
                px_exists = os.path.exists(os.path.join(tmp_dir, f"{name}.PX"))
                if not px_exists and file_size > 2048:
                    cprint(YELLOW, f"    Warning: No .PX index file — this may be why 0 rows were read")
        except Exception as e:
            cprint(RED, f"  {name:30s}  READ FAILED (from copy)")
            print_error(f"reading {name}.DB (copied)", e)

            # Fallback: try reading directly from the network share
            direct_path = os.path.join(saveurs_path, f"{name}.DB")
            try:
                cprint(YELLOW, f"    Trying direct read from network share...")
                direct_size = os.path.getsize(direct_path)
                rows = read_table(direct_path)
                results[name] = rows
                cols = list(rows[0].keys()) if rows else []
                if rows:
                    cprint(GREEN, f"    DIRECT READ OK: {len(rows)} rows, {len(cols)} columns ({direct_size/1024:.1f} KB)")
                else:
                    cprint(YELLOW, f"    Direct read returned 0 rows ({direct_size/1024:.1f} KB)")

                    # Hex dump first 128 bytes for debugging
                    with open(direct_path, "rb") as hf:
                        header = hf.read(128)
                    cprint(DIM, f"    Header hex dump (first 128 bytes):")
                    for row_off in range(0, len(header), 16):
                        hex_part = " ".join(f"{b:02x}" for b in header[row_off:row_off+16])
                        cprint(DIM, f"      {row_off:04x}: {hex_part}")
            except Exception as e2:
                cprint(RED, f"    Direct read also FAILED")
                print_error(f"direct reading {name}.DB", e2)
                errors[name] = f"Copy failed: {e} | Direct failed: {e2}"
        finally:
            cleanup(tmp_dir)

    # Summary
    print()
    total = len(tables)
    ok = len(results)
    fail = len(errors)
    cprint(BOLD, f"  Summary: {ok}/{total} tables read OK, {fail} failed")

    if errors:
        cprint(RED, f"\n  Failed tables:")
        for name, err in errors.items():
            cprint(RED, f"    {name}: {err}")

    return results


# ---------------------------------------------------------------------------
# Test 4: Key tables for ProtoCart
# ---------------------------------------------------------------------------

KEY_TABLES = ["ARTICLES", "NOTE_ENTETE", "NOTE_DETAIL"]


def test_key_tables(cached: dict[str, list[dict]]):
    separator("=")
    cprint(BOLD, " TEST 4: Key tables for ProtoCart")
    separator("=")

    all_ok = True
    for table in KEY_TABLES:
        rows = cached.get(table)
        if rows is None:
            cprint(RED, f"  {table}: NOT READ — this table is REQUIRED for the bridge to work!")
            all_ok = False
            continue

        if len(rows) == 0:
            cprint(YELLOW, f"  {table}: 0 rows — table exists but is empty")
            cprint(YELLOW, f"    If POS has been used, this likely means the Paradox reader")
            cprint(YELLOW, f"    couldn't parse the data. Check if .PX file exists.")
            all_ok = False
            continue

        print(f"\n  {CYAN}{table}{RESET}  ({len(rows)} rows)")
        cols = list(rows[0].keys())
        print(f"  Columns ({len(cols)}): {', '.join(cols)}")

        # Validate expected columns exist
        if table == "ARTICLES":
            expected = ["ART_ID", "ART_ARTICLE", "ART_BARCODE"]
            _check_expected_cols(cols, expected, table)
        elif table == "NOTE_ENTETE":
            expected = ["VTE_ID", "VTE_TOTAL_TTC", "VTE_DATE_DE_LA"]
            _check_expected_cols(cols, expected, table)
        elif table == "NOTE_DETAIL":
            expected = ["VTE_ID", "ART_ID", "VTE_QUANTITE", "VTE_PRIX_DE_V"]
            _check_expected_cols(cols, expected, table)

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

    print()
    if all_ok:
        cprint(GREEN, "  All 3 key tables OK — bridge should work!")
        cprint(DIM, "  NOTE: NOTE_ENTETE/NOTE_DETAIL hold ACTIVE receipts.")
        cprint(DIM, "  After daily closing (cloture), data moves to VENTE_REGLEE/ARTICLE_VENDU.")
    else:
        cprint(RED, "  Some key tables are missing or empty — bridge will NOT work correctly.")
        cprint(YELLOW, "  Share the test_report.log file for debugging.")


def _check_expected_cols(actual_cols: list[str], expected_prefixes: list[str], table: str):
    """Check that expected column name prefixes exist in the actual column list."""
    upper_cols = [c.upper() for c in actual_cols]
    for prefix in expected_prefixes:
        found = any(c.startswith(prefix.upper()) for c in upper_cols)
        if not found:
            cprint(RED, f"    MISSING expected column: {prefix} in {table}")
            cprint(YELLOW, f"    Available columns: {', '.join(actual_cols)}")
            cprint(YELLOW, f"    Column names may differ from expected — check Paradox viewer")


# ---------------------------------------------------------------------------
# Test 5: V2 discovery
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
                cols = list(rows[0].keys()) if rows else ["(not read)"]
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
            val = None
            matched_key = None
            for k in sample:
                if k.upper().startswith(field.upper()[:12]):
                    val = sample[k]
                    matched_key = k
                    break
            if matched_key:
                samples = [r.get(matched_key) for r in art_rows[:5]]
                non_zero = [s for s in samples if s and s != 0 and s != 0.0]
                status = GREEN if non_zero else DIM
                print(f"    {status}{matched_key:25s} samples: {samples}{RESET}")


# ---------------------------------------------------------------------------
# Test 6: Quick API endpoint smoke test
# ---------------------------------------------------------------------------

def test_api_smoke():
    separator("=")
    cprint(BOLD, " TEST 6: API module import check")
    separator("=")

    checks = [
        ("app.main", "app"),
        ("app.routers.health", "router"),
        ("app.routers.sales", "router"),
        ("app.routers.articles", "router"),
        ("app.services.sales_service", "get_sales"),
        ("app.services.articles_service", "get_articles"),
        ("app.services.file_manager", "safe_copy_tables"),
        ("app.utils.date_utils", "parse_iso"),
    ]

    all_ok = True
    for module_path, attr_name in checks:
        try:
            mod = __import__(module_path, fromlist=[attr_name])
            obj = getattr(mod, attr_name)
            cprint(GREEN, f"  {module_path}.{attr_name}: OK")
        except Exception as e:
            cprint(RED, f"  {module_path}.{attr_name}: FAILED")
            print_error(f"importing {module_path}.{attr_name}", e)
            all_ok = False

    return all_ok


# ---------------------------------------------------------------------------
# Interactive query mode
# ---------------------------------------------------------------------------

def interactive_mode(tables: list[str], cached: dict[str, list[dict]], saveurs_path: str):
    from app.services.paradox_reader import read_table

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
    {CYAN}sales today{RESET}              Sales for today (04:00 -> 23:59)
    {CYAN}fc2 <path> <from> <to>{RESET}  Read sales from FC2 file (dates as YYYY-MM-DD)
    {CYAN}fc2 <path> list{RESET}         List journals in FC2 file
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

        try:
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
                    tmp_dir, copy_err = safe_copy_single(tname, saveurs_path)
                    if copy_err:
                        cprint(RED, f"  Cannot read {tname}: {copy_err}")
                        continue
                    try:
                        rows = read_table(os.path.join(tmp_dir, f"{tname}.DB"))
                        cached[tname] = rows
                    except Exception as e:
                        print_error(f"reading {tname}", e)
                        continue
                    finally:
                        cleanup(tmp_dir)

                rows = cached[tname]
                last_result = rows
                limit = len(rows) if show_all else min(20, len(rows))
                print(f"  {tname}: {len(rows)} rows (showing {limit})\n")

                if rows:
                    cols = list(rows[0].keys())
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
                else:
                    cprint(YELLOW, f"  {tname}: 0 rows (empty)")

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

                matched_col = None
                for k in rows[0]:
                    if k.upper().startswith(col_search):
                        matched_col = k
                        break
                if not matched_col:
                    cprint(RED, f"  Column starting with '{col_search}' not found.")
                    cprint(YELLOW, f"  Available: {', '.join(rows[0].keys())}")
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

                if len(parts) >= 3:
                    from_str = parts[1]
                    to_str = parts[2]
                elif len(parts) == 2 and parts[1].lower() == "today":
                    tz = get_tz()
                    today = datetime.now(tz).date()
                    from_str = f"{today}T04:00:00"
                    to_str = f"{today}T23:59:59"
                else:
                    cprint(YELLOW, "  Usage: sales <from> <to>  or  sales today")
                    cprint(YELLOW, "  Dates as YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS")
                    continue

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
                print(f"  Period:             {meta['periodFrom']} -> {meta['periodTo']}")
                print()

                if sales:
                    print(f"  {'Article':30s} {'Qty':>8s} {'Revenue':>10s} {'UnitPrice':>10s} {'Txns':>6s}")
                    separator()
                    for s in sorted(sales, key=lambda x: x["totalRevenue"], reverse=True):
                        name = (s["articleName"] or "?")[:30]
                        print(f"  {name:30s} {s['quantitySold']:8.1f} {s['totalRevenue']:10.2f} {s['unitPrice']:10.2f} {s['transactionCount']:6d}")
                else:
                    cprint(YELLOW, "  No sales found for this period.")
                    cprint(YELLOW, "  Check: Are there receipts in NOTE_ENTETE for this date?")
                    cprint(YELLOW, "  Try: read NOTE_ENTETE   then look at VTE_DATE_DE_LA_PIECE values")
                    cprint(YELLOW, "  Tip: After daily closing, data is purged. Try 'sales today'.")

            # ---- fc2 ----
            elif cmd == "fc2" and len(parts) >= 2:
                from app.services.fc2_reader import (
                    extract_journals, parse_journal_lines,
                    get_journal_sales, parse_jv_filename,
                    list_fc2_files,
                )
                from app.services.sales_service import _aggregate_journal_lines

                fc2_path = parts[1]

                if not os.path.isfile(fc2_path):
                    # Maybe it's a directory — find FC2 files
                    if os.path.isdir(fc2_path):
                        fc2_files = list_fc2_files(fc2_path)
                        if fc2_files:
                            cprint(GREEN, f"  Found {len(fc2_files)} FC2 files:")
                            for f in fc2_files:
                                size_mb = os.path.getsize(f) / 1024 / 1024
                                mtime = datetime.fromtimestamp(os.path.getmtime(f))
                                print(f"    {os.path.basename(f):40s}  {size_mb:6.1f} MB  modified: {mtime.strftime('%Y-%m-%d %H:%M')}")
                        else:
                            cprint(RED, f"  No .FC2 files found in {fc2_path}")
                        continue
                    cprint(RED, f"  File not found: {fc2_path}")
                    continue

                if len(parts) >= 3 and parts[2].lower() == "list":
                    # List journals in FC2 file
                    cprint(DIM, f"  Reading {fc2_path}...")
                    journals = extract_journals(fc2_path, "JV")
                    print(f"\n  {CYAN}Sales Journals (JV){RESET}: {len(journals)} files\n")
                    for fname in sorted(journals.keys()):
                        parsed = parse_jv_filename(fname)
                        lines_count = len(journals[fname].strip().split("\n")) - 1
                        month_label = f"  ({parsed[0]:02d}/{parsed[1]})" if parsed else ""
                        print(f"    {fname:20s}  {lines_count:6d} data lines{month_label}")

                    jr_journals = extract_journals(fc2_path, "JR")
                    print(f"\n  {CYAN}Payment Journals (JR){RESET}: {len(jr_journals)} files")

                elif len(parts) >= 4:
                    # fc2 <path> <from> <to>
                    from_str = parts[2]
                    to_str = parts[3]
                    if "T" not in from_str:
                        from_str += "T00:00:00"
                    if "T" not in to_str:
                        to_str += "T23:59:59"

                    from app.utils.date_utils import parse_iso
                    from_dt = parse_iso(from_str)
                    to_dt = parse_iso(to_str)

                    cprint(DIM, f"  Reading FC2: {os.path.basename(fc2_path)}")
                    cprint(DIM, f"  Period: {from_dt.date()} to {to_dt.date()}...")

                    sale_lines = get_journal_sales(fc2_path, from_dt, to_dt)
                    result = _aggregate_journal_lines(sale_lines)
                    sales_data = result["sales"]
                    last_result = sales_data

                    print()
                    cprint(BOLD, f"  FC2 Sales Results: {len(sales_data)} articles sold")
                    separator()
                    print(f"  Total transactions: {result['totalTransactions']}")
                    print(f"  Total revenue:      {result['totalRevenue']:,.2f} DH")
                    print(f"  Raw line items:     {len(sale_lines)}")

                    if sale_lines:
                        dates = sorted(set(l["date"].date() for l in sale_lines))
                        print(f"  Days with data:     {len(dates)} ({dates[0]} to {dates[-1]})")

                    print()
                    if sales_data:
                        print(f"  {'Article':30s} {'Qty':>8s} {'Revenue':>10s} {'Price':>8s} {'Txns':>6s}  {'Category'}")
                        separator()
                        for s in sorted(sales_data, key=lambda x: x["totalRevenue"], reverse=True)[:30]:
                            name = (s["articleName"] or "?")[:30]
                            cat = (s.get("classification") or "")[:20]
                            print(f"  {name:30s} {s['quantitySold']:8.1f} {s['totalRevenue']:10.2f} {s['unitPrice']:8.2f} {s['transactionCount']:6d}  {cat}")
                        if len(sales_data) > 30:
                            print(f"  ... and {len(sales_data) - 30} more articles")
                    else:
                        cprint(YELLOW, "  No sales found for this period in the FC2 file.")
                else:
                    cprint(YELLOW, "  Usage: fc2 <path> list")
                    cprint(YELLOW, "         fc2 <path> <from-date> <to-date>")
                    cprint(YELLOW, "  Example: fc2 C:\\path\\to\\file.FC2 2024-11-01 2024-11-30")

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

        except Exception as e:
            print_error(f"executing '{raw}'", e)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    os.system("")  # Enable ANSI colors on Windows

    print()
    cprint(BOLD + CYAN, "  ╔══════════════════════════════════════════════════════╗")
    cprint(BOLD + CYAN, "  ║   Pointex Bridge — Interactive Tester               ║")
    cprint(BOLD + CYAN, "  ║   All output saved to: test_report.log              ║")
    cprint(BOLD + CYAN, "  ╚══════════════════════════════════════════════════════╝")
    print()
    print(f"  Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Test 0: Environment
    if not test_environment():
        cprint(RED, "\n  Environment check failed. Fix the issues above first.")
        cprint(YELLOW, f"  Full log saved to: {LOG_FILE}")
        input("\n  Press Enter to exit...")
        return

    print()

    # Test 6: API module imports
    if not test_api_smoke():
        cprint(RED, "\n  API module import check failed.")
        cprint(YELLOW, f"  Full log saved to: {LOG_FILE}")
        input("\n  Press Enter to exit...")
        return

    print()

    # Load settings
    from app.config import settings
    saveurs_path = settings.saveurs_path

    # Test 1: Connection
    if not test_connection():
        cprint(RED, "\n  Cannot proceed without access to SAVEURS path.")
        cprint(YELLOW, "  Fix the path in .env or ensure CAISSE-PC is reachable.")
        cprint(YELLOW, f"\n  Full log saved to: {LOG_FILE}")
        input("\n  Press Enter to exit...")
        return

    print()

    # Test 2: Discover tables
    tables = discover_tables(saveurs_path)
    if not tables:
        cprint(RED, "\n  No .DB files found!")
        cprint(YELLOW, f"\n  Full log saved to: {LOG_FILE}")
        input("\n  Press Enter to exit...")
        return

    print()
    cprint(BOLD, "  Reading all tables? This scans every .DB file to verify readability.")
    choice = input(f"  Read all {len(tables)} tables? [Y/n] ").strip().lower()
    if choice in ("", "y", "yes"):
        cached = test_read_tables(tables, saveurs_path)
        print()
        test_key_tables(cached)
        print()
        test_v2_discovery(tables, cached)
    else:
        cprint(DIM, "  Skipping full scan. Reading key tables only...")
        cached = test_read_tables(KEY_TABLES, saveurs_path)

    print()
    cprint(GREEN, f"  Test report saved to: {LOG_FILE}")
    cprint(YELLOW, f"  If anything failed, share this file for debugging.")
    print()

    # Interactive mode
    interactive_mode(tables, cached, saveurs_path)

    print()
    cprint(GREEN, "  Done. Goodbye!")
    cprint(GREEN, f"  Full session log: {LOG_FILE}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print_error("FATAL — unhandled exception in main()", e)
        cprint(RED, f"\n  Something went completely wrong.")
        cprint(YELLOW, f"  Please share the file: {LOG_FILE}")
        input("\n  Press Enter to exit...")
