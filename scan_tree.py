"""
Scan the SAVEURS directory tree and dump structure + Paradox table info.
Run at the bakery: python scan_tree.py > tree_output.txt
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from app.services.paradox_reader import read_table


def scan_dir(base_path, output_file=None):
    out = output_file or sys.stdout

    def pr(msg=""):
        print(msg, file=out)

    pr(f"=== SCAN: {base_path} ===")
    pr(f"Date: {__import__('datetime').datetime.now().isoformat()}")
    pr()

    # Phase 1: Full directory tree
    pr("=" * 70)
    pr("PHASE 1: DIRECTORY TREE")
    pr("=" * 70)

    db_files = []
    txt_files = []

    for root, dirs, files in os.walk(base_path):
        # Skip very deep paths
        depth = root.replace(base_path, "").count(os.sep)
        if depth > 3:
            continue

        rel = os.path.relpath(root, base_path)
        indent = "  " * depth
        pr(f"{indent}{rel}/")

        for f in sorted(files):
            full = os.path.join(root, f)
            try:
                size = os.path.getsize(full)
                mtime = os.path.getmtime(full)
                mdate = __import__('datetime').datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M")
            except OSError:
                size = 0
                mdate = "?"

            size_str = f"{size:>10,}"
            ext = os.path.splitext(f)[1].upper()
            pr(f"{indent}  {f:45s} {size_str} bytes  {mdate}")

            if ext == ".DB":
                db_files.append(full)
            if ext == ".TXT":
                txt_files.append(full)

    # Phase 2: Read every .DB file header (row count + columns)
    pr()
    pr("=" * 70)
    pr("PHASE 2: PARADOX TABLE DETAILS")
    pr("=" * 70)

    for db_path in sorted(db_files):
        rel = os.path.relpath(db_path, base_path)
        pr(f"\n--- {rel} ---")
        try:
            rows = read_table(db_path)
            pr(f"  Rows: {len(rows)}")
            if rows:
                cols = list(rows[0].keys())
                pr(f"  Columns ({len(cols)}): {', '.join(cols)}")
                # Show first row values
                pr(f"  Sample row:")
                for k, v in rows[0].items():
                    val_str = str(v)[:80]
                    pr(f"    {k:35s} = {val_str}")
                # If there's a date column, show range
                for col in cols:
                    if "DATE" in col.upper():
                        dates = [r[col] for r in rows if r.get(col) is not None]
                        if dates:
                            pr(f"  Date range ({col}): {min(dates)} -> {max(dates)}")
            else:
                pr(f"  (empty table)")
        except Exception as e:
            pr(f"  ERROR: {e}")

    # Phase 3: Check TXT files (journal-like?)
    if txt_files:
        pr()
        pr("=" * 70)
        pr("PHASE 3: TEXT FILES")
        pr("=" * 70)

        for txt_path in sorted(txt_files):
            rel = os.path.relpath(txt_path, base_path)
            size = os.path.getsize(txt_path)
            pr(f"\n--- {rel} ({size:,} bytes) ---")
            try:
                with open(txt_path, "r", encoding="cp1252", errors="replace") as f:
                    lines = f.readlines()
                pr(f"  Lines: {len(lines)}")
                if lines:
                    pr(f"  Header: {lines[0].strip()[:150]}")
                if len(lines) > 1:
                    pr(f"  Line 1: {lines[1].strip()[:150]}")
                if len(lines) > 2:
                    pr(f"  Line 2: {lines[2].strip()[:150]}")
            except Exception as e:
                pr(f"  ERROR: {e}")

    pr()
    pr("=== SCAN COMPLETE ===")
    pr(f"Total .DB files: {len(db_files)}")
    pr(f"Total .TXT files: {len(txt_files)}")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else r"\\CAISSE-PC\firstclass\SAVEURS"
    outfile = sys.argv[2] if len(sys.argv) > 2 else None

    if outfile:
        with open(outfile, "w", encoding="utf-8") as f:
            scan_dir(path, f)
        print(f"Saved to {outfile}")
    else:
        scan_dir(path)
