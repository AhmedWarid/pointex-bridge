"""
Paradox .DB file reader with fallback strategy.

Tries libraries in order:
1. paradox-reader (pure Python, most portable)
2. pypxlib (C wrapper, faster)
3. Manual binary parsing (last resort)

The reader returns rows as list[dict] with string keys matching column names.
"""

import logging
import os
import struct
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

_reader_impl = None


def read_table(db_path: str) -> list[dict]:
    """Read a Paradox .DB file and return rows as list of dicts."""
    global _reader_impl

    # Validate the file before attempting to read
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"File not found: {db_path}")

    file_size = os.path.getsize(db_path)
    if file_size == 0:
        raise ValueError(
            f"File is 0 bytes (empty): {db_path}. "
            f"This usually means the file copy failed due to a lock by the POS software, "
            f"or the source file itself is empty."
        )
    if file_size < 88:  # Minimum Paradox header size
        raise ValueError(
            f"File is too small to be a valid Paradox file ({file_size} bytes): {db_path}"
        )

    if _reader_impl is None:
        _reader_impl = _pick_reader()
    return _reader_impl(db_path)


def _pick_reader():
    """Try available Paradox readers in order of preference."""
    # 1. paradox-reader (pure Python)
    try:
        from paradox_reader import ParadoxReader as _PR  # noqa: F401

        logger.info("Using paradox-reader (pure Python)")
        return _read_with_paradox_reader
    except ImportError:
        pass

    # 2. pypxlib
    try:
        import pypxlib  # noqa: F401

        logger.info("Using pypxlib (C wrapper)")
        return _read_with_pypxlib
    except ImportError:
        pass

    # 3. Fallback: manual binary parser
    logger.info("Using manual Paradox binary parser (fallback)")
    return _read_manual


# ---------------------------------------------------------------------------
# Implementation 1: paradox-reader
# ---------------------------------------------------------------------------


def _read_with_paradox_reader(db_path: str) -> list[dict]:
    from paradox_reader import ParadoxReader

    reader = ParadoxReader(db_path)
    fields = reader.fields
    field_names = [f.name for f in fields]
    rows = []
    for record in reader:
        row = {}
        for i, name in enumerate(field_names):
            row[name] = record[i]
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Implementation 2: pypxlib
# ---------------------------------------------------------------------------


def _read_with_pypxlib(db_path: str) -> list[dict]:
    from pypxlib import Table

    table = Table(db_path)
    field_names = table.fields
    rows = []
    for record in table:
        row = {}
        for name in field_names:
            row[name] = getattr(record, name, None)
        rows.append(row)
    table.close()
    return rows


# ---------------------------------------------------------------------------
# Implementation 3: Manual binary parser for Paradox 7
# ---------------------------------------------------------------------------

# Paradox field type constants
_PX_ALPHA = 0x01
_PX_DATE = 0x02
_PX_SHORT = 0x03
_PX_LONG = 0x04
_PX_CURRENCY = 0x05
_PX_NUMBER = 0x06
_PX_LOGICAL = 0x09
_PX_MEMO_BLOB = 0x0C
_PX_BLOB = 0x0D
_PX_FMTMEMO = 0x0E
_PX_OLE = 0x0F
_PX_GRAPHIC = 0x10
_PX_TIMESTAMP = 0x14
_PX_AUTOINC = 0x16
_PX_BCD = 0x17
_PX_BYTES = 0x18

# Paradox epoch: Jan 1, year 1 (Delphi TDateTime compatible)
_PARADOX_DATE_EPOCH = datetime(1, 1, 1)


def _read_manual(db_path: str) -> list[dict]:
    """Parse a Paradox .DB file from raw bytes."""
    with open(db_path, "rb") as f:
        data = f.read()

    file_size = len(data)
    if file_size < 0x58:
        raise ValueError(
            f"File too small for Paradox header ({file_size} bytes, need >= 88): {db_path}"
        )

    # ---- HEADER ----
    # Offset 0x00: record size (2 bytes, little-endian)
    record_size = struct.unpack_from("<H", data, 0x00)[0]
    # Offset 0x02: header size (2 bytes, little-endian)
    header_size = struct.unpack_from("<H", data, 0x02)[0]
    # Offset 0x04: file type (1 byte)
    file_type = data[0x04]
    # Offset 0x05: max table size in KB (1 byte)
    max_table_size = data[0x05]
    # Offset 0x06: number of records (4 bytes, little-endian signed)
    num_records = struct.unpack_from("<i", data, 0x06)[0]
    # Offset 0x0A: number of blocks used (2 bytes)
    num_blocks = struct.unpack_from("<H", data, 0x0A)[0]
    # Offset 0x0C: number of blocks in file (2 bytes)
    num_blocks_total = struct.unpack_from("<H", data, 0x0C)[0]
    # Offset 0x0E: first block (2 bytes)
    first_block = struct.unpack_from("<H", data, 0x0E)[0]
    # Offset 0x10: last block (2 bytes)
    last_block = struct.unpack_from("<H", data, 0x10)[0]
    # Offset 0x21: number of fields (1 byte) — in Paradox 4+
    num_fields_byte = data[0x21]
    # Offset 0x38: number of fields (2 bytes) — more reliable for Paradox 7
    num_fields_word = struct.unpack_from("<H", data, 0x38)[0]
    # Use the 2-byte field count if available, else fallback to 1-byte
    num_fields = num_fields_word if num_fields_word > 0 else num_fields_byte

    logger.debug(
        "Paradox header: record_size=%d header_size=%d file_type=%d "
        "max_table_size=%d num_records=%d num_fields=%d first_block=%d last_block=%d",
        record_size, header_size, file_type, max_table_size,
        num_records, num_fields, first_block, last_block,
    )

    if num_records <= 0 or num_fields <= 0:
        logger.info("Table has 0 records or 0 fields: %s", db_path)
        return []

    if record_size == 0:
        raise ValueError(
            f"Invalid Paradox file: record_size is 0. "
            f"Header may be corrupt or this isn't a Paradox .DB file: {db_path}"
        )

    # ---- FIELD DESCRIPTORS ----
    # In Paradox 7 (.DB file type 0x00..0x03 for non-keyed, or with keys):
    # Field info array starts at offset 0x78.
    # Each field descriptor is 2 bytes: type (1 byte) + size (1 byte)
    # BUT the actual layout varies by Paradox version.
    #
    # Standard Paradox 7 layout:
    #   Offset 0x58: field type array — num_fields entries, each 1 byte (type)
    #   Offset 0x58 + num_fields: field size array — num_fields entries, each 2 bytes (size LE)
    #
    # Alternative (what some files use):
    #   Offset 0x78: packed 4-byte entries (type, size_lo, size_hi, 0x00)
    #
    # We try the standard layout first, validate, then fallback to packed.

    fields = _parse_field_descriptors(data, num_fields, record_size, db_path)

    if not fields:
        raise ValueError(
            f"Could not parse field descriptors from {db_path}. "
            f"num_fields={num_fields}, file_size={file_size}, header_size={header_size}"
        )

    # Validate: sum of field sizes should equal record_size
    total_field_size = sum(fsize for _, fsize in fields)
    if total_field_size != record_size:
        logger.warning(
            "Field size mismatch: sum(field_sizes)=%d but record_size=%d in %s. "
            "Will attempt to read anyway.",
            total_field_size, record_size, db_path,
        )

    # ---- FIELD NAMES ----
    # Field names follow the field descriptor arrays.
    # They're null-terminated ASCII strings packed sequentially.
    # The exact offset depends on which descriptor layout was used.
    # We calculate it from the end of the field descriptors.
    field_names = _parse_field_names(data, num_fields, header_size, db_path)

    if len(field_names) != num_fields:
        # Fallback: generate placeholder names
        logger.warning(
            "Expected %d field names but found %d in %s. Using placeholders.",
            num_fields, len(field_names), db_path,
        )
        while len(field_names) < num_fields:
            field_names.append(f"FIELD_{len(field_names)+1}")

    # ---- DATA BLOCKS ----
    data_start = header_size
    rows = []
    block_size = max_table_size * 1024  # max_table_size is in KB
    if block_size == 0:
        block_size = 4096  # reasonable default

    offset = data_start
    records_read = 0

    while offset < file_size and records_read < num_records:
        if offset + 6 > file_size:
            break

        # Block header: next_block (2), prev_block (2), last_record_in_block (2, signed)
        _next_block = struct.unpack_from("<H", data, offset)[0]
        _prev_block = struct.unpack_from("<H", data, offset + 2)[0]
        last_rec = struct.unpack_from("<h", data, offset + 4)[0]

        if last_rec < 0:
            # Empty or invalid block, skip to next
            offset += block_size
            continue

        num_recs_in_block = last_rec + 1
        rec_offset = offset + 6

        for _ in range(num_recs_in_block):
            if records_read >= num_records:
                break
            if rec_offset + record_size > file_size:
                break

            row = _parse_record(data, rec_offset, fields, field_names)
            rows.append(row)
            rec_offset += record_size
            records_read += 1

        offset += block_size

    logger.info("Read %d/%d records from %s", records_read, num_records, db_path)
    return rows


def _parse_field_descriptors(data: bytes, num_fields: int, record_size: int, db_path: str) -> list[tuple[int, int]]:
    """
    Try multiple strategies to parse field descriptors.
    Returns list of (field_type, field_size) tuples.
    """
    file_size = len(data)

    # Strategy 1: Standard Paradox 7 layout
    # Types at 0x78, each 1 byte. Sizes at 0x78 + num_fields, each 2 bytes LE.
    type_offset = 0x78
    size_offset = type_offset + num_fields
    needed = size_offset + num_fields * 2
    if needed <= file_size:
        fields = []
        for i in range(num_fields):
            ftype = data[type_offset + i]
            fsize = struct.unpack_from("<H", data, size_offset + i * 2)[0]
            fields.append((ftype, fsize))

        total = sum(s for _, s in fields)
        # Check if types look valid (all between 1 and 0x20)
        types_valid = all(1 <= t <= 0x25 for t, _ in fields)
        sizes_valid = all(0 < s < 10000 for _, s in fields)

        if types_valid and sizes_valid and total == record_size:
            logger.debug("Field descriptors: Strategy 1 (standard) matched")
            return fields

        if types_valid and sizes_valid and abs(total - record_size) < 10:
            logger.debug("Field descriptors: Strategy 1 (standard) close match (diff=%d)", total - record_size)
            return fields

    # Strategy 2: Packed 4-byte entries at 0x78 (type, size_lo, size_hi, padding)
    packed_offset = 0x78
    needed = packed_offset + num_fields * 4
    if needed <= file_size:
        fields = []
        for i in range(num_fields):
            base = packed_offset + i * 4
            ftype = data[base]
            fsize = struct.unpack_from("<H", data, base + 1)[0]
            fields.append((ftype, fsize))

        total = sum(s for _, s in fields)
        types_valid = all(1 <= t <= 0x25 for t, _ in fields)
        sizes_valid = all(0 < s < 10000 for _, s in fields)

        if types_valid and sizes_valid and total == record_size:
            logger.debug("Field descriptors: Strategy 2 (packed 4-byte) matched")
            return fields

    # Strategy 3: Types and sizes at 0x58 (older Paradox versions)
    type_offset = 0x58
    size_offset = type_offset + num_fields
    needed = size_offset + num_fields * 2
    if needed <= file_size:
        fields = []
        for i in range(num_fields):
            ftype = data[type_offset + i]
            fsize = struct.unpack_from("<H", data, size_offset + i * 2)[0]
            fields.append((ftype, fsize))

        total = sum(s for _, s in fields)
        types_valid = all(1 <= t <= 0x25 for t, _ in fields)
        sizes_valid = all(0 < s < 10000 for _, s in fields)

        if types_valid and sizes_valid and total == record_size:
            logger.debug("Field descriptors: Strategy 3 (offset 0x58) matched")
            return fields

    # Nothing matched — log details for debugging
    logger.error(
        "Could not parse field descriptors for %s. "
        "num_fields=%d record_size=%d file_size=%d. "
        "Header hex dump (first 128 bytes): %s",
        db_path, num_fields, record_size, file_size,
        data[:128].hex(),
    )
    return []


def _parse_field_names(data: bytes, num_fields: int, header_size: int, db_path: str) -> list[str]:
    """
    Find and parse field names from the Paradox header.
    Field names are null-terminated ASCII strings packed sequentially.
    Their exact offset varies, so we scan for them.
    """
    file_size = len(data)

    # The field names are typically in the second half of the header,
    # after the field descriptor arrays. We look for sequences of
    # printable ASCII followed by null bytes.
    #
    # Common start offsets to try:
    # - 0x78 + num_fields * 4 (after packed descriptors)
    # - 0x78 + num_fields + num_fields * 2 (after standard descriptors)
    # - 0x78 + num_fields * 3 (variant)

    candidates = [
        0x78 + num_fields * 4,           # after packed 4-byte descriptors
        0x78 + num_fields + num_fields * 2,  # after type[1] + size[2] arrays
        0x78 + num_fields * 3,           # variant
        0x58 + num_fields + num_fields * 2,  # older layout at 0x58
    ]

    for start in candidates:
        if start >= header_size or start >= file_size:
            continue
        names = _try_parse_names_at(data, start, num_fields, header_size)
        if len(names) == num_fields:
            return names

    # Brute force: scan from 0x78 onwards looking for the first null-terminated string sequence
    for start in range(0x78, min(header_size, file_size - num_fields)):
        if data[start] < 0x20 or data[start] > 0x7E:
            continue
        names = _try_parse_names_at(data, start, num_fields, header_size)
        if len(names) == num_fields:
            return names

    logger.warning("Could not locate field names in %s", db_path)
    return []


def _try_parse_names_at(data: bytes, offset: int, num_fields: int, header_size: int) -> list[str]:
    """Try to parse num_fields null-terminated names starting at offset."""
    names = []
    pos = offset
    limit = min(header_size, len(data))
    for _ in range(num_fields):
        if pos >= limit:
            break
        try:
            end = data.index(0x00, pos, limit)
        except ValueError:
            break
        name = data[pos:end].decode("ascii", errors="replace").strip()
        if not name or not all(c.isalnum() or c in "_- " for c in name):
            break
        names.append(name)
        pos = end + 1
    return names


def _parse_record(
    data: bytes, offset: int, fields: list, field_names: list
) -> dict:
    """Parse a single Paradox record from raw bytes."""
    row = {}
    pos = offset
    for i, (ftype, fsize) in enumerate(fields):
        name = field_names[i] if i < len(field_names) else f"FIELD_{i+1}"
        raw = data[pos : pos + fsize]

        if ftype == _PX_ALPHA:
            val = raw.rstrip(b"\x00").decode("cp1252", errors="replace").rstrip()
            row[name] = val if val else None
        elif ftype == _PX_DATE:
            if fsize == 4 and raw != b"\x00" * 4:
                dval = struct.unpack_from(">i", raw, 0)[0]
                dval ^= 0x80000000
                try:
                    row[name] = _PARADOX_DATE_EPOCH + timedelta(days=dval - 1)
                except (OverflowError, ValueError):
                    row[name] = None
            else:
                row[name] = None
        elif ftype == _PX_SHORT:
            if fsize >= 2:
                val = struct.unpack_from(">h", raw, 0)[0]
                val ^= 0x8000
                row[name] = val
            else:
                row[name] = 0
        elif ftype in (_PX_LONG, _PX_AUTOINC):
            if fsize >= 4:
                val = struct.unpack_from(">i", raw, 0)[0]
                val ^= 0x80000000
                row[name] = val
            else:
                row[name] = 0
        elif ftype == _PX_CURRENCY:
            if fsize == 8:
                val = struct.unpack_from(">q", raw, 0)[0]
                val ^= 0x8000000000000000
                row[name] = val / 10000.0
            else:
                row[name] = 0.0
        elif ftype == _PX_NUMBER:
            if fsize == 8:
                raw_bytes = bytearray(raw)
                if raw_bytes[0] & 0x80:
                    raw_bytes[0] ^= 0x80
                else:
                    raw_bytes = bytearray(b ^ 0xFF for b in raw_bytes)
                val = struct.unpack(">d", bytes(raw_bytes))[0]
                row[name] = val
            else:
                row[name] = 0.0
        elif ftype == _PX_LOGICAL:
            row[name] = raw[0] != 0 if fsize >= 1 else None
        elif ftype == _PX_TIMESTAMP:
            if fsize == 8 and raw != b"\x00" * 8:
                raw_bytes = bytearray(raw)
                if raw_bytes[0] & 0x80:
                    raw_bytes[0] ^= 0x80
                else:
                    raw_bytes = bytearray(b ^ 0xFF for b in raw_bytes)
                ts = struct.unpack(">d", bytes(raw_bytes))[0]
                try:
                    days = ts / 86400000.0
                    row[name] = _PARADOX_DATE_EPOCH + timedelta(days=days - 1)
                except (OverflowError, ValueError):
                    row[name] = None
            else:
                row[name] = None
        elif ftype in (_PX_MEMO_BLOB, _PX_BLOB, _PX_FMTMEMO, _PX_OLE, _PX_GRAPHIC):
            row[name] = None
        elif ftype == _PX_BCD:
            row[name] = 0
        elif ftype == _PX_BYTES:
            row[name] = raw
        else:
            row[name] = raw

        pos += fsize

    return row
