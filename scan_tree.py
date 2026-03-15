"""
Scan the SAVEURS directory tree — compact output.
Run at the bakery: python scan_tree.py > tree_output.txt
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))

from app.services.paradox_reader import read_table


def scan_dir(base_path, out=None):
    out = out or sys.stdout
    def pr(msg=""):
        print(msg, file=out)

    pr(f"SCAN: {base_path}  ({datetime.now().strftime('%Y-%m-%d %H:%M')})")
    pr()

    db_files = []

    # Phase 1: Tree — only show .DB and .TXT, skip .PX/.MB/.XG/.VAL
    for root, dirs, files in os.walk(base_path):
        depth = root.replace(base_path, "").count(os.sep)
        if depth > 3:
            continue

        rel = os.path.relpath(root, base_path)
        interesting = [f for f in files if f.upper().endswith((".DB", ".TXT", ".FC2"))]
        other_count = len(files) - len(interesting)

        if not interesting and not dirs:
            continue

        indent = "  " * depth
        pr(f"{indent}[{rel}]")

        for f in sorted(interesting):
            full = os.path.join(root, f)
            try:
                sz = os.path.getsize(full)
                mt = datetime.fromtimestamp(os.path.getmtime(full)).strftime("%m/%d %H:%M")
            except OSError:
                sz = 0
                mt = "?"
            kb = f"{sz // 1024}K"
            pr(f"{indent}  {f:40s} {kb:>7s}  {mt}")
            if f.upper().endswith(".DB"):
                db_files.append(full)

        if other_count:
            pr(f"{indent}  (+{other_count} other files)")

    # Phase 2: Read each .DB — one line per table if empty, details if has rows
    pr()
    pr("=" * 60)
    pr(f"TABLES ({len(db_files)} .DB files)")
    pr("=" * 60)

    for db_path in sorted(db_files):
        rel = os.path.relpath(db_path, base_path)
        try:
            rows = read_table(db_path)
            n = len(rows)
            if n == 0:
                pr(f"  {rel:50s}  EMPTY")
            else:
                cols = list(rows[0].keys())
                pr(f"\n  {rel}  [{n} rows, {len(cols)} cols]")
                pr(f"    cols: {', '.join(cols)}")
                # Show first row compactly
                parts = []
                for k, v in rows[0].items():
                    s = str(v)[:40]
                    parts.append(f"{k}={s}")
                pr(f"    row0: {' | '.join(parts)}")
                # Date range if any
                for col in cols:
                    if "DATE" in col.upper():
                        dates = [r[col] for r in rows if r.get(col)]
                        if dates:
                            pr(f"    {col}: {min(dates)} -> {max(dates)}")
        except Exception as e:
            pr(f"  {rel:50s}  ERR: {e}")

    pr(f"\nDone. {len(db_files)} tables scanned.")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else r"\\CAISSE-PC\firstclass\SAVEURS"
    outfile = sys.argv[2] if len(sys.argv) > 2 else None

    if outfile:
        with open(outfile, "w", encoding="utf-8") as f:
            scan_dir(path, f)
        print(f"Saved to {outfile}")
    else:
        scan_dir(path)
