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
_PX_TIMESTAMP = 0x14
_PX_AUTOINC = 0x16
_PX_BCD = 0x17

# Paradox epoch: Jan 1, year 1 (Delphi TDateTime compatible)
_PARADOX_DATE_EPOCH = datetime(1, 1, 1)
# Paradox timestamps use Jan 1, 0001 with milliseconds since midnight
_DELPHI_EPOCH_OFFSET = 693594  # days between 0001-01-01 and 1899-12-30


def _read_manual(db_path: str) -> list[dict]:
    """Parse a Paradox 7 .DB file from raw bytes."""
    with open(db_path, "rb") as f:
        data = f.read()

    if len(data) < 0x58:
        return []

    # Header fields
    record_size = struct.unpack_from("<H", data, 0x00)[0]
    header_size = struct.unpack_from("<H", data, 0x02)[0]
    file_type = data[0x04]
    max_table_size = data[0x05]
    num_records = struct.unpack_from("<i", data, 0x06)[0]
    num_fields = struct.unpack_from("<H", data, 0x22)[0]

    if num_records <= 0 or num_fields <= 0:
        return []

    # Field descriptors start at offset 0x58
    # Each descriptor: type (1 byte) + size (1 byte) = but Paradox 7 uses
    # 2-byte type + 2-byte size pairs at offset 0x78
    # The actual layout: field info array starts at 0x78, each entry is 4 bytes
    field_offset = 0x78
    fields = []
    for i in range(num_fields):
        ftype = data[field_offset + i * 4]
        fsize = struct.unpack_from("<H", data, field_offset + i * 4 + 1)[0]
        fields.append((ftype, fsize))

    # Field names are after the field info array, stored as null-terminated strings
    names_offset = field_offset + num_fields * 4
    field_names = []
    pos = names_offset
    for _ in range(num_fields):
        end = data.index(0x00, pos)
        name = data[pos:end].decode("ascii", errors="replace")
        field_names.append(name)
        pos = end + 1

    # Data blocks start after the header
    data_start = header_size
    rows = []

    # Paradox stores data in blocks. Each block has a header:
    # next_block (2 bytes), prev_block (2 bytes), last_record_in_block (2 bytes)
    # Then records follow.
    block_size = max_table_size * 1024  # max_table_size is in KB
    offset = data_start
    records_read = 0

    while offset < len(data) and records_read < num_records:
        if offset + 6 > len(data):
            break

        # Block header
        _next_block = struct.unpack_from("<H", data, offset)[0]
        _prev_block = struct.unpack_from("<H", data, offset + 2)[0]
        last_rec = struct.unpack_from("<h", data, offset + 4)[0]

        num_recs_in_block = last_rec + 1
        rec_offset = offset + 6

        for _ in range(num_recs_in_block):
            if records_read >= num_records:
                break
            if rec_offset + record_size > len(data):
                break

            row = _parse_record(data, rec_offset, fields, field_names)
            rows.append(row)
            rec_offset += record_size
            records_read += 1

        offset += block_size

    return rows


def _parse_record(
    data: bytes, offset: int, fields: list, field_names: list
) -> dict:
    """Parse a single Paradox record from raw bytes."""
    row = {}
    pos = offset
    for i, (ftype, fsize) in enumerate(fields):
        name = field_names[i]
        raw = data[pos : pos + fsize]

        if ftype == _PX_ALPHA:
            val = raw.rstrip(b"\x00").decode("cp1252", errors="replace").rstrip()
            row[name] = val if val else None
        elif ftype == _PX_DATE:
            if fsize == 4 and raw != b"\x00" * 4:
                # Paradox date: 4-byte signed int, days since Jan 1, 0001
                # High bit is flipped for sorting
                dval = struct.unpack_from(">i", raw, 0)[0]
                dval ^= 0x80000000
                try:
                    row[name] = _PARADOX_DATE_EPOCH + timedelta(days=dval - 1)
                except (OverflowError, ValueError):
                    row[name] = None
            else:
                row[name] = None
        elif ftype == _PX_SHORT:
            val = struct.unpack_from(">h", raw, 0)[0]
            val ^= 0x8000  # flip sign bit
            row[name] = val
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
                row[name] = val / 10000.0  # currency stored as fixed-point
            else:
                row[name] = 0.0
        elif ftype == _PX_NUMBER:
            if fsize == 8:
                # IEEE 754 double with high bit flipped
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
                # Timestamp: double as days since 0001-01-01 (like Delphi TDateTime)
                raw_bytes = bytearray(raw)
                if raw_bytes[0] & 0x80:
                    raw_bytes[0] ^= 0x80
                else:
                    raw_bytes = bytearray(b ^ 0xFF for b in raw_bytes)
                ts = struct.unpack(">d", bytes(raw_bytes))[0]
                try:
                    # Convert from Paradox timestamp (ms since 0001-01-01)
                    # to Python datetime
                    days = ts / 86400000.0
                    row[name] = _PARADOX_DATE_EPOCH + timedelta(days=days - 1)
                except (OverflowError, ValueError):
                    row[name] = None
            else:
                row[name] = None
        elif ftype in (_PX_MEMO_BLOB, _PX_BLOB):
            # Memo/BLOB data lives in .MB file — skip for now
            row[name] = None
        elif ftype == _PX_BCD:
            row[name] = 0  # BCD not commonly used
        else:
            row[name] = raw

        pos += fsize

    return row
