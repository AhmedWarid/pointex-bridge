"""
Generate fake Paradox .DB files for local API testing.

Creates a directory structure at ./test-data/SAVEURS/ that mimics
the POS machine, so you can point SAVEURS_PATH to it and test all endpoints.

Usage:
    python generate_test_data.py
    # Then set SAVEURS_PATH=./test-data/SAVEURS in .env
"""

import os
import struct
from datetime import date

# ---------------------------------------------------------------------------
# Paradox binary writer helpers
# ---------------------------------------------------------------------------

# Sign-bit flip for Paradox integers (big-endian storage)
_I_COMP = 1 << 31
_H_COMP = 1 << 15

def _pack_int(val: int | None) -> bytes:
    """Pack a 32-bit Paradox integer (sign-bit flipped, big-endian)."""
    if val is None or val == 0:
        return b'\x00\x00\x00\x00'
    # Flip sign bit: XOR with 0x80000000
    raw = struct.pack('>i', val)
    b0 = raw[0] ^ 0x80
    return bytes([b0]) + raw[1:]

def _pack_short(val: int | None) -> bytes:
    """Pack a 16-bit Paradox short (sign-bit flipped, big-endian)."""
    if val is None or val == 0:
        return b'\x00\x00'
    raw = struct.pack('>h', val)
    b0 = raw[0] ^ 0x80
    return bytes([b0]) + raw[1:]

def _pack_double(val: float | None) -> bytes:
    """Pack a Paradox double (negated, big-endian)."""
    if val is None or val == 0.0:
        return b'\x00' * 8
    # The reader does: return -struct.unpack('>d', s)[0]
    # So to store value V, we write -V
    return struct.pack('>d', -val)

def _pack_alpha(val: str, size: int) -> bytes:
    """Pack a Paradox alpha string (cp1252, null-padded)."""
    encoded = val.encode('cp1252', errors='replace')[:size]
    return encoded.ljust(size, b'\x00')

def _pack_date(d: date) -> bytes:
    """Pack a Python date as Paradox date (ordinal as 32-bit int)."""
    return _pack_int(d.toordinal())


def write_paradox_db(
    filepath: str,
    table_name: str,
    fields: list[tuple[str, int, int]],  # (name, type_byte, size)
    rows: list[list],  # each row = list of raw values matching field order
):
    """
    Write a minimal valid Paradox .DB file.

    fields: list of (field_name, paradox_type_byte, field_size_bytes)
    rows: list of lists, each inner list has one value per field
    """
    os.makedirs(os.path.dirname(filepath) or '.', exist_ok=True)

    num_fields = len(fields)
    record_size = sum(f[2] for f in fields)
    block_size_kb = 4
    block_size = block_size_kb * 1024

    # --- Build header ---
    # Field descriptors at 0x78, 2 bytes each (type + size)
    field_defs = b''
    for _, type_byte, size in fields:
        field_defs += bytes([type_byte, size])

    # Table name + field names (null-terminated)
    name_section = table_name.upper().encode('cp1252') + b'\x00'
    for fname, _, _ in fields:
        name_section += fname.encode('ascii') + b'\x00'

    # Header = 0x78 bytes fixed + field_defs + name_section, padded to alignment
    header_content = bytearray(0x78)
    header_content += field_defs
    header_content += name_section

    # Pad header to a reasonable boundary
    header_size = len(header_content)
    # Align to 4 bytes
    if header_size % 4 != 0:
        header_size += 4 - (header_size % 4)

    header = bytearray(header_size)
    header[:len(header_content)] = header_content

    # Fill header fields
    num_records = len(rows)
    struct.pack_into('<H', header, 0, record_size)         # record_size
    struct.pack_into('<H', header, 2, header_size)          # header_size
    header[5] = block_size_kb                               # block_size_kb

    struct.pack_into('<i', header, 6, num_records)          # num_records

    # Block navigation
    num_blocks = 1 if num_records > 0 else 0
    struct.pack_into('<H', header, 0x0A, num_blocks)        # blocks used
    struct.pack_into('<H', header, 0x0C, num_blocks)        # blocks total
    struct.pack_into('<H', header, 0x0E, 1 if num_blocks else 0)  # first block
    struct.pack_into('<H', header, 0x10, 1 if num_blocks else 0)  # last block

    header[0x21] = num_fields                               # num_fields

    # --- Build data block ---
    block = bytearray(block_size)
    # Block header: next(2) + prev(2) + addDataSize(2)
    struct.pack_into('<H', block, 0, 0)  # next = 0 (no next)
    struct.pack_into('<H', block, 2, 0)  # prev = 0

    if num_records > 0:
        add_data_size = (num_records - 1) * record_size
        struct.pack_into('<h', block, 4, add_data_size)
    else:
        struct.pack_into('<h', block, 4, -1)  # empty block

    # Write records into block
    offset = 6
    for row in rows:
        for i, (_, type_byte, size) in enumerate(fields):
            val = row[i]
            if type_byte == 0x01:      # Alpha
                block[offset:offset+size] = _pack_alpha(val or '', size)
            elif type_byte == 0x04:    # Long int
                block[offset:offset+size] = _pack_int(val)
            elif type_byte == 0x03:    # Short int
                block[offset:offset+size] = _pack_short(val)
            elif type_byte == 0x06:    # Double
                block[offset:offset+size] = _pack_double(val)
            elif type_byte == 0x02:    # Date
                block[offset:offset+size] = _pack_date(val) if val else b'\x00' * size
            else:
                block[offset:offset+size] = b'\x00' * size
            offset += size

    # --- Write file ---
    with open(filepath, 'wb') as f:
        f.write(header)
        if num_records > 0:
            f.write(block)

    print(f"  Created {filepath} ({num_records} records)")


# ---------------------------------------------------------------------------
# Test data definitions
# ---------------------------------------------------------------------------

BASE_DIR = os.path.join(os.path.dirname(__file__), "test-data", "SAVEURS")

# Categories
CATEGORIES = [
    (1, "Viennoiserie"),
    (2, "Pain"),
    (3, "Patisserie"),
    (4, "Boissons"),
    (5, "Traiteur"),
]

# Articles: (ART_ID, ART_CODE, ART_NOM, ART_PVTE (selling price), CLS_ID)
ARTICLES = [
    (1001, "001", "Croissant",           1.50,  1),
    (1002, "002", "Pain au chocolat",    1.80,  1),
    (1003, "003", "Baguette tradition",  1.20,  2),
    (1004, "004", "Pain complet",        1.50,  2),
    (1005, "005", "Tarte aux pommes",    3.50,  3),
    (1006, "006", "Eclair cafe",         2.80,  3),
    (1007, "007", "Jus d'orange",        2.50,  4),
    (1008, "008", "Cafe",                1.20,  4),
    (1009, "009", "Quiche lorraine",     4.00,  5),
    (1010, "010", "Sandwich poulet",     5.50,  5),
    (1011, "011", "Baghrir",             1.00,  2),
    (1012, "012", "Pastilla poulet",     8.00,  5),
    (1013, "013", "Mini nems legumes",   3.00,  5),
]

# Yesterday's archived sales (VD = detail lines)
# (VTE_ID, ART_ID, VTE_QTE, VTE_PRIX, VTE_TYPE_LIGNE, VTE_CACHE, VTE_COMPOSTAGE)
YESTERDAY = date(2026, 3, 16)
YESTERDAY_SALES = [
    # Receipt 1 (VTE_ID=5001)
    (5001, 1001,  3,  1.50, 0, 0, 1),   # 3 Croissants
    (5001, 1002,  2,  1.80, 0, 0, 1),   # 2 Pain au chocolat
    (5001, 1008,  1,  1.20, 0, 0, 1),   # 1 Cafe
    # Receipt 2 (VTE_ID=5002)
    (5002, 1003,  2,  1.20, 0, 0, 2),   # 2 Baguettes
    (5002, 1005,  1,  3.50, 0, 0, 2),   # 1 Tarte aux pommes
    # Receipt 3 (VTE_ID=5003) — with a voided line
    (5003, 1011, 10,  1.00, 0, 0, 3),   # 10 Baghrir
    (5003, 1012,  4,  8.00, 0, 0, 3),   # 4 Pastilla poulet
    (5003, 1013,  6,  3.00, 0, 0, 3),   # 6 Mini nems
    (5003, 1009,  1,  4.00, 0, 1, 3),   # VOIDED — 1 Quiche (VTE_CACHE=1)
    # Receipt 4 (VTE_ID=5004) — has discount line (type=1)
    (5004, 1006,  2,  2.80, 0, 0, 4),   # 2 Eclairs
    (5004, 1007,  3,  2.50, 0, 0, 4),   # 3 Jus d'orange
    (5004, 0,     0, -1.00, 1, 0, 4),   # DISCOUNT line (type=1, skip)
]

# Yesterday's archive headers (VE = entetes/headers)
# (VTE_ID, VTE_COMPOSTAGE, VTE_DATE)
YESTERDAY_HEADERS = [
    (5001, 1, YESTERDAY),
    (5002, 2, YESTERDAY),
    (5003, 3, YESTERDAY),
    (5004, 4, YESTERDAY),
]

# Today's live data (NOTE_ENTETE + NOTE_DETAIL) — simulates before Z closing
TODAY = date(2026, 3, 17)
TODAY_HEADERS = [
    (6001, 1, TODAY),
    (6002, 2, TODAY),
]
TODAY_SALES = [
    (6001, 1001,  5,  1.50, 0, 0, 1),   # 5 Croissants
    (6001, 1003,  3,  1.20, 0, 0, 1),   # 3 Baguettes
    (6002, 1011, 15,  1.00, 0, 0, 2),   # 15 Baghrir
    (6002, 1009,  2,  4.00, 0, 0, 2),   # 2 Quiche
]


def main():
    print("Generating test Paradox data...\n")
    os.makedirs(BASE_DIR, exist_ok=True)

    # --- CLASSIFICATION.DB ---
    cls_fields = [
        ("CLS_ID",             0x04, 4),   # Long int
        ("CLS_CLASSIFICATION", 0x01, 50),  # Alpha
    ]
    cls_rows = [[cid, cname] for cid, cname in CATEGORIES]
    write_paradox_db(
        os.path.join(BASE_DIR, "CLASSIFICATION.DB"),
        "CLASSIFICATION", cls_fields, cls_rows,
    )

    # --- ARTICLES.DB ---
    art_fields = [
        ("ART_ID",      0x06, 8),   # Double (Pointex stores IDs as floats)
        ("ART_CODE",    0x01, 20),  # Alpha
        ("ART_ARTICLE", 0x01, 60),  # Alpha — this is the real POS column name
        ("ART_PVTE",    0x06, 8),   # Double (selling price)
        ("CLS_ID",      0x04, 4),   # Long int
    ]
    art_rows = [[float(aid), code, nom, prix, cid]
                for aid, code, nom, prix, cid in ARTICLES]
    write_paradox_db(
        os.path.join(BASE_DIR, "ARTICLES.DB"),
        "ARTICLES", art_fields, art_rows,
    )

    # --- Yesterday's archive: AN2026/VD031626.DB ---
    vd_fields = [
        ("VTE_ID",              0x06, 8),   # Double
        ("ART_ID",              0x06, 8),   # Double
        ("VTE_QUANTITE",        0x06, 8),   # Double (quantity)
        ("VTE_PRIX_DE_VENTE",   0x06, 8),   # Double (unit price)
        ("VTE_TYPE_LIGNE",      0x04, 4),   # Long int
        ("VTE_CACHE",           0x04, 4),   # Long int
        ("VTE_COMPOSTAGE",      0x04, 4),   # Long int
    ]
    vd_rows = [[float(vid), float(aid), float(qty), prix, tl, vc, comp]
               for vid, aid, qty, prix, tl, vc, comp in YESTERDAY_SALES]
    archive_dir = os.path.join(BASE_DIR, f"AN{YESTERDAY.year}")
    vd_name = f"VD{YESTERDAY.strftime('%m%d%y')}.DB"
    write_paradox_db(
        os.path.join(archive_dir, vd_name),
        vd_name.replace('.DB', ''), vd_fields, vd_rows,
    )

    # --- Yesterday's archive headers: AN2026/VE031626.DB ---
    ve_fields = [
        ("VTE_ID",            0x06, 8),   # Double
        ("VTE_COMPOSTAGE",    0x04, 4),   # Long int
        ("VTE_DATE_DE_LA_VE", 0x02, 4),   # Date
    ]
    ve_rows = [[float(vid), comp, dt]
               for vid, comp, dt in YESTERDAY_HEADERS]
    ve_name = f"VE{YESTERDAY.strftime('%m%d%y')}.DB"
    write_paradox_db(
        os.path.join(archive_dir, ve_name),
        ve_name.replace('.DB', ''), ve_fields, ve_rows,
    )

    # --- Today's live data: NOTE_ENTETE.DB ---
    write_paradox_db(
        os.path.join(BASE_DIR, "NOTE_ENTETE.DB"),
        "NOTE_ENTETE", ve_fields,
        [[float(vid), comp, dt] for vid, comp, dt in TODAY_HEADERS],
    )

    # --- Today's live data: NOTE_DETAIL.DB ---
    write_paradox_db(
        os.path.join(BASE_DIR, "NOTE_DETAIL.DB"),
        "NOTE_DETAIL", vd_fields,
        [[float(vid), float(aid), float(qty), prix, tl, vc, comp]
         for vid, aid, qty, prix, tl, vc, comp in TODAY_SALES],
    )

    print(f"\nDone! Test data at: {os.path.abspath(BASE_DIR)}")
    print(f"\nTo use it, update your .env:")
    print(f"  SAVEURS_PATH={os.path.abspath(BASE_DIR)}")
    print(f"\nExpected results:")
    print(f"  GET /api/pos/articles -> {len(ARTICLES)} articles with categories")
    print(f"  GET /api/pos/sales/{YESTERDAY} -> archive data, 4 receipts")
    print(f"  GET /api/pos/sales/{TODAY} -> live data, 2 receipts")


if __name__ == "__main__":
    main()
