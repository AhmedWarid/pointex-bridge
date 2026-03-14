"""
FC2 Reader — Extract sales journals from Pointex First Class 2 backup files.

FC2 files (.FC2) are Pointex's proprietary backup format containing:
  1. A custom header with business info
  2. Zlib-compressed Paradox table snapshots
  3. ZIP-compressed journal text files (JV=sales, JR=payments) at the end

Journal files are tab-delimited text (cp1252) with 31 columns.
JV files = Journal des Ventes (Sales Journal), one per month.
Naming: JV{MM}{DD}{YY}.TXT  (e.g. JV010124.TXT = January 2024)
"""

import logging
import os
import struct
import zlib
from datetime import datetime

logger = logging.getLogger(__name__)

# --- ZIP local file header parsing ---
# We parse manually because the FC2 ZIP section is concatenated
# after binary Paradox data, and Python's zipfile module can't
# always handle the offset correctly.

_PK_LOCAL = b"PK\x03\x04"
_LOCAL_HEADER_FMT = "<HHHHHIIIHH"  # 26 bytes after signature
_LOCAL_HEADER_SIZE = struct.calcsize(_LOCAL_HEADER_FMT)


def _find_zip_entries(data: bytes) -> list[tuple[str, int, int, int, int]]:
    """
    Scan binary data for ZIP local file headers.
    Returns list of (filename, method, comp_size, uncomp_size, data_offset).
    """
    entries = []
    pos = 0
    while True:
        idx = data.find(_PK_LOCAL, pos)
        if idx == -1:
            break
        try:
            (ver, flags, method, mtime, mdate, crc,
             comp_size, uncomp_size, name_len, extra_len) = struct.unpack_from(
                _LOCAL_HEADER_FMT, data, idx + 4
            )
            name_start = idx + 4 + _LOCAL_HEADER_SIZE
            name = data[name_start:name_start + name_len].decode("cp1252")
            data_offset = name_start + name_len + extra_len
            entries.append((name, method, comp_size, uncomp_size, data_offset))
            pos = data_offset + comp_size
        except (struct.error, UnicodeDecodeError):
            pos = idx + 4
    return entries


def _decompress_entry(data: bytes, method: int, comp_size: int, data_offset: int) -> bytes:
    """Decompress a single ZIP entry."""
    comp_data = data[data_offset:data_offset + comp_size]
    if method == 8:  # deflate
        return zlib.decompress(comp_data, -15)
    elif method == 0:  # stored
        return comp_data
    else:
        raise ValueError(f"Unsupported compression method: {method}")


def list_fc2_files(fc2_dir: str) -> list[str]:
    """
    Find all .FC2 files in a directory, sorted by modification time (newest first).
    """
    fc2_files = []
    try:
        for f in os.listdir(fc2_dir):
            if f.upper().endswith(".FC2"):
                full = os.path.join(fc2_dir, f)
                fc2_files.append(full)
    except (OSError, PermissionError) as e:
        logger.warning("Cannot list FC2 directory %s: %s", fc2_dir, e)
        return []

    fc2_files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return fc2_files


def extract_journals(fc2_path: str, prefix: str = "JV") -> dict[str, str]:
    """
    Extract journal text files from an FC2 backup.

    Args:
        fc2_path: Path to the .FC2 file
        prefix: "JV" for sales journals, "JR" for payment journals

    Returns:
        dict mapping filename (e.g. "JV010124.TXT") to decoded text content.
    """
    logger.info("Reading FC2 file: %s", fc2_path)

    with open(fc2_path, "rb") as f:
        data = f.read()

    entries = _find_zip_entries(data)
    logger.info("Found %d ZIP entries in FC2 file", len(entries))

    journals = {}
    prefix_upper = prefix.upper()
    for name, method, comp_size, uncomp_size, data_offset in entries:
        if not name.upper().startswith(prefix_upper):
            continue
        try:
            raw = _decompress_entry(data, method, comp_size, data_offset)
            text = raw.decode("cp1252")
            journals[name] = text
        except Exception as e:
            logger.warning("Failed to extract %s: %s", name, e)

    logger.info("Extracted %d %s journal files", len(journals), prefix)
    return journals


def parse_jv_filename(filename: str) -> tuple[int, int] | None:
    """
    Parse a JV filename like JV010124.TXT into (month, year).
    Format: JV{MM}{DD}{YY}.TXT where DD is always 01.
    Returns (month, 4-digit-year) or None.
    """
    name = filename.upper().replace(".TXT", "")
    if not name.startswith("JV") or len(name) != 8:
        return None
    try:
        mm = int(name[2:4])
        yy = int(name[6:8])
        year = 2000 + yy if yy < 80 else 1900 + yy
        return (mm, year)
    except ValueError:
        return None


def _parse_french_float(val: str) -> float:
    """Parse a French-format number (comma as decimal separator)."""
    if not val or not val.strip():
        return 0.0
    try:
        return float(val.strip().replace(",", "."))
    except (ValueError, TypeError):
        return 0.0


def parse_journal_lines(text: str) -> list[dict]:
    """
    Parse a tab-delimited JV journal into a list of sale line dicts.

    Each dict has normalized keys:
        date, receipt_id, line_order, service_name, order_time,
        line_type, line_type_label, quantity, unit_price, discount_pct,
        tva_rate, amount_ht, article_name, article_code,
        classification, cls_code, zone_name, unit_name
    """
    lines = text.strip().split("\n")
    if len(lines) < 2:
        return []

    headers = [h.strip().strip('"') for h in lines[0].split("\t")]

    # Build column index map (case-insensitive)
    col = {}
    for i, h in enumerate(headers):
        col[h.upper()] = i

    results = []
    for line in lines[1:]:
        fields = [f.strip().strip('"') for f in line.split("\t")]
        if len(fields) < 16:
            continue

        def get(name: str) -> str:
            idx = col.get(name.upper())
            if idx is not None and idx < len(fields):
                return fields[idx]
            return ""

        # Only keep article lines (VTE_TYPE_LIGNE = 0)
        line_type = get("VTE_TYPE_LIGNE")
        if line_type != "0":
            continue

        # Parse date YYYYMMDD
        raw_date = get("DATE")
        try:
            dt = datetime.strptime(raw_date, "%Y%m%d")
        except (ValueError, TypeError):
            continue

        qty = _parse_french_float(get("VTE_QUANTITE"))
        price = _parse_french_float(get("VTE_PRIX_DE_VENTE"))
        discount = _parse_french_float(get("VTE_REMISE"))
        tva = _parse_french_float(get("VTE_TVA"))
        amount_ht = _parse_french_float(get("VTE_MONTANT_HT"))

        results.append({
            "date": dt,
            "receipt_id": get("VTE_COMPOSTAGE"),
            "line_order": get("VTE_ORDRE"),
            "service_name": get("SRV_LIBELLE"),
            "order_time": get("VTE_HEURE_COMMANDE"),
            "line_type": line_type,
            "line_type_label": get("VTE_TYPE_LIGNE_LIBELLE"),
            "quantity": qty,
            "unit_price": price,
            "discount_pct": discount,
            "tva_rate": tva,
            "amount_ht": amount_ht,
            "article_name": get("ART_LIBELLE"),
            "article_code": get("ART_CODE"),
            "classification": get("CLS_CLASSIFICATION"),
            "cls_code": get("CLS_CODE"),
            "zone_name": get("ZST_NOM"),
            "unit_name": get("UNT_UNITE"),
        })

    return results


def get_journal_sales(
    fc2_path: str,
    from_dt: datetime,
    to_dt: datetime,
) -> list[dict]:
    """
    Extract and filter sales lines from an FC2 file for a date range.

    Returns a flat list of sale line dicts (one per line item).
    """
    journals = extract_journals(fc2_path, prefix="JV")

    # Filter to only journals that MIGHT overlap the date range
    relevant = {}
    for fname, text in journals.items():
        parsed = parse_jv_filename(fname)
        if parsed is None:
            relevant[fname] = text
            continue
        mm, yyyy = parsed
        # Journal covers one month — include if it overlaps the range
        journal_start = datetime(yyyy, mm, 1)
        if mm == 12:
            journal_end = datetime(yyyy + 1, 1, 1)
        else:
            journal_end = datetime(yyyy, mm + 1, 1)

        if journal_start <= to_dt and journal_end >= from_dt:
            relevant[fname] = text

    logger.info(
        "Processing %d/%d journals for period %s -> %s",
        len(relevant), len(journals),
        from_dt.date().isoformat(), to_dt.date().isoformat(),
    )

    all_lines = []
    for fname, text in relevant.items():
        lines = parse_journal_lines(text)
        for line in lines:
            line_date = line["date"]
            if from_dt.date() <= line_date.date() <= to_dt.date():
                all_lines.append(line)

    logger.info("Found %d sale lines in date range", len(all_lines))
    return all_lines
