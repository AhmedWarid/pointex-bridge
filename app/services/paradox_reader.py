"""
Paradox .DB file reader.

Based on Bertrand Bordage's proven pure-Python Paradox reader
(https://gist.github.com/BertrandBordage/9892556) adapted for
Pointex/Paradox 7 files with cp1252 encoding.

The reader returns rows as list[dict] with string keys matching column names.

IMPORTANT: All companion files (.DB, .PX, .MB, .XG0, .XG1, .XG2, .YG0, .VAL)
must be in the same directory for the reader to work correctly.
"""

import logging
import os
import struct
from datetime import date, datetime, timedelta

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Reader selection
# ---------------------------------------------------------------------------

_reader_impl = None


def read_table(db_path: str) -> list[dict]:
    """Read a Paradox .DB file and return rows as list of dicts."""
    global _reader_impl

    if not os.path.exists(db_path):
        raise FileNotFoundError(f"File not found: {db_path}")

    file_size = os.path.getsize(db_path)
    if file_size == 0:
        raise ValueError(
            f"File is 0 bytes (empty): {db_path}. "
            f"The file copy likely failed because the POS has it locked."
        )
    if file_size < 10:
        raise ValueError(f"File too small ({file_size} bytes): {db_path}")

    if _reader_impl is None:
        _reader_impl = _pick_reader()
    return _reader_impl(db_path)


def _pick_reader():
    """Try available Paradox readers in order of preference."""
    # 1. pypxlib (C wrapper, most reliable if available)
    try:
        import pypxlib  # noqa: F401
        logger.info("Using pypxlib (C wrapper)")
        return _read_with_pypxlib
    except ImportError:
        pass

    # 2. Built-in pure Python reader (based on Bertrand Bordage's implementation)
    logger.info("Using built-in pure Python Paradox reader")
    return _read_paradox


# ---------------------------------------------------------------------------
# Implementation 1: pypxlib
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
# Implementation 2: Pure Python Paradox reader
# Based on https://gist.github.com/BertrandBordage/9892556
# ---------------------------------------------------------------------------

# Paradox field type codes
FIELD_TYPES = {
    0x01: 'A',   # Alpha (string)
    0x02: 'D',   # Date
    0x03: 'S',   # Short integer (16-bit)
    0x04: 'I',   # Long integer (32-bit)
    0x05: '$',   # Currency/Money
    0x06: 'N',   # Number (double)
    0x09: 'L',   # Logical (boolean)
    0x0C: 'M',   # Memo
    0x0D: 'B',   # BLOB
    0x0E: 'F',   # Formatted memo
    0x0F: 'O',   # OLE
    0x10: 'G',   # Graphic
    0x14: 'T',   # Time
    0x15: '@',   # Timestamp
    0x16: '+',   # Autoincrement
    0x17: 'BCD', # BCD
    0x18: 'Y',   # Bytes
}

INPUT_ENCODING = 'cp1252'

# ---------------------------------------------------------------------------
# Byte unpacking helpers (Paradox stores integers with flipped sign bit)
# ---------------------------------------------------------------------------

_I_COMP = 1 << 31  # 2^31
_H_COMP = 1 << 15  # 2^15
_B_COMP = 1 << 7   # 2^7


def _unpack_i(s):
    """Unpack a 4-byte big-endian Paradox integer (sign bit flipped)."""
    v = struct.unpack('>i', s)[0]
    if v == 0:
        return None
    return v - _I_COMP if v > 0 else v + _I_COMP


def _unpack_h(s):
    """Unpack a 2-byte big-endian Paradox short (sign bit flipped)."""
    v = struct.unpack('>h', s)[0]
    if v == 0:
        return None
    return v - _H_COMP if v > 0 else v + _H_COMP


def _unpack_b(s):
    """Unpack a 1-byte Paradox logical/boolean."""
    v = struct.unpack('>b', s)[0]
    if v == 0:
        return None
    return bool((v - _B_COMP) if v > 0 else (v + _B_COMP))


def _unpack_d(s):
    """Unpack an 8-byte Paradox double (sign bit flipped)."""
    return -struct.unpack('>d', s)[0]


def _to_date(s):
    """Convert 4-byte Paradox date to Python date."""
    ordinal = _unpack_i(s)
    if ordinal is None:
        return None
    try:
        return date.fromordinal(ordinal)
    except (ValueError, OverflowError):
        return None


def _to_time(s):
    """Convert 4-byte Paradox time to Python time."""
    ms = _unpack_i(s)
    if ms is None:
        return None
    seconds = ms // 1000
    return (datetime.min + timedelta(seconds=seconds)).time()


_SECONDS_PER_DAY = 86400


def _to_datetime(s):
    """Convert 8-byte Paradox timestamp to Python datetime."""
    raw = _unpack_d(s)
    if raw is None or raw == 0:
        return None
    try:
        ms = int(raw)
        total_seconds = ms // 1000
        ordinal, seconds = divmod(total_seconds, _SECONDS_PER_DAY)
        d = datetime.fromordinal(ordinal)
        return d + timedelta(seconds=seconds)
    except (ValueError, OverflowError, OSError):
        return None


def _decode_field(field_type: str, raw: bytes) -> object:
    """Decode a single field value from raw bytes."""
    if all(b == 0 for b in raw):
        return None

    try:
        if field_type == 'A':
            return raw.rstrip(b'\x00').decode(INPUT_ENCODING, errors='replace').rstrip()
        elif field_type == 'S':
            return _unpack_h(raw)
        elif field_type == 'L':
            return _unpack_b(raw)
        elif field_type in ('I', '+'):
            return _unpack_i(raw)
        elif field_type == 'D':
            return _to_date(raw)
        elif field_type == 'T':
            return _to_time(raw)
        elif field_type == '@':
            return _to_datetime(raw)
        elif field_type in ('N', '$'):
            return _unpack_d(raw)
        elif field_type in ('M', 'B', 'F', 'O', 'G'):
            return None  # Memo/BLOB — stored in .MB file
        elif field_type == 'Y':
            return raw  # Raw bytes
        else:
            return raw
    except (struct.error, ValueError, OverflowError) as e:
        logger.debug("Error decoding field type %s: %s", field_type, e)
        return None


# ---------------------------------------------------------------------------
# Main reader
# ---------------------------------------------------------------------------

def _read_paradox(db_path: str) -> list[dict]:
    """
    Read a Paradox .DB file and return rows as list[dict].

    Based on Bertrand Bordage's implementation with improvements for
    robustness and Paradox 7 compatibility.
    """
    filename = os.path.basename(db_path)
    table_name = os.path.splitext(filename)[0]

    with open(db_path, 'rb') as f:
        # Read first 6 bytes to get record_size and header_size
        preamble = f.read(6)
        if len(preamble) < 6:
            raise ValueError(f"File too small: {db_path}")

        record_size = struct.unpack('<H', preamble[0:2])[0]
        header_size_raw = struct.unpack('<H', preamble[2:4])[0]
        block_size_kb = preamble[5]

        # Calculate actual header size in bytes
        # Paradox stores header size in different units depending on version
        # Try the value directly first, then common multipliers
        header_bytes = header_size_raw

        # Read the full header
        f.seek(0)
        header = f.read(header_bytes)

        if len(header) < 78:
            raise ValueError(
                f"Header too small ({len(header)} bytes) in {db_path}. "
                f"record_size={record_size}, header_size_raw={header_size_raw}"
            )

        # Number of records
        num_records = struct.unpack('<i', header[6:10])[0]
        # Number of fields — byte at offset 0x21 (33)
        num_fields = header[0x21]

        logger.info(
            "%s: record_size=%d, header_bytes=%d, block_size=%dKB, "
            "num_records=%d, num_fields=%d",
            table_name, record_size, header_bytes, block_size_kb,
            num_records, num_fields,
        )

        if num_fields == 0:
            logger.warning("%s: 0 fields in header", table_name)
            return []

        # --- Parse field types and sizes ---
        # Field info starts at offset 0x78 (120)
        # Each field: 1 byte type + 1 byte size = 2 bytes per field
        field_defs_offset = 0x78
        fields = []
        for i in range(num_fields):
            off = field_defs_offset + i * 2
            if off + 2 > len(header):
                raise ValueError(
                    f"Header too short to contain field {i+1}/{num_fields} "
                    f"at offset {off} (header is {len(header)} bytes) in {db_path}"
                )
            type_byte = header[off]
            size_byte = header[off + 1]

            field_type = FIELD_TYPES.get(type_byte, '?')
            if field_type == '?' or size_byte == 0:
                logger.warning(
                    "%s: unknown field type 0x%02x size %d at index %d",
                    table_name, type_byte, size_byte, i,
                )
            fields.append((field_type, size_byte))

        # Validate: sum of field sizes should match record_size
        calc_record_size = sum(s for _, s in fields)
        if calc_record_size != record_size:
            logger.warning(
                "%s: field sizes sum to %d but record_size is %d. "
                "Trying alternative field descriptor layout...",
                table_name, calc_record_size, record_size,
            )
            # Try alternative: types as 1-byte array, then sizes as 2-byte LE array
            alt_fields = _try_alt_field_layout(header, num_fields, record_size, table_name)
            if alt_fields:
                fields = alt_fields
                calc_record_size = sum(s for _, s in fields)

        if calc_record_size != record_size:
            logger.error(
                "%s: CANNOT match field sizes (%d) to record_size (%d). "
                "Field types: %s. Header hex (0x78+): %s",
                table_name, calc_record_size, record_size,
                [(t, s) for t, s in fields],
                header[0x78:0x78 + num_fields * 4].hex(),
            )
            raise ValueError(
                f"Field descriptor mismatch in {db_path}: "
                f"sum(field_sizes)={calc_record_size} != record_size={record_size}. "
                f"This file may use an unsupported Paradox format variant."
            )

        # --- Parse field names ---
        # Field names are null-terminated strings after the field descriptors.
        # Method 1: Split header by the table name (appears in header as marker)
        field_names = _extract_field_names_by_tablename(header, table_name, num_fields)

        # Method 2: Scan after field descriptors
        if len(field_names) != num_fields:
            names_start = field_defs_offset + num_fields * 2
            field_names = _extract_field_names_by_scan(header, names_start, num_fields, header_bytes)

        # Method 3: Generate placeholders
        if len(field_names) != num_fields:
            logger.warning(
                "%s: found %d names for %d fields, using placeholders",
                table_name, len(field_names), num_fields,
            )
            field_names = [f"FIELD_{i+1}" for i in range(num_fields)]

        logger.info("%s: columns = %s", table_name, field_names)

        # --- Read data blocks ---
        block_size = block_size_kb * 1024
        if block_size == 0:
            block_size = 4096

        data_start = header_bytes
        f.seek(0, 2)
        file_size = f.tell()

        # Read firstBlock pointer from file header (offset 0x0E, 2 bytes LE)
        # Paradox files use a linked list of data blocks — we must follow it
        # to avoid reading deleted/free blocks that contain garbage data.
        first_block_num = struct.unpack('<H', header[0x0E:0x10])[0] if len(header) >= 0x10 else 0

        rows = []
        records_read = 0
        max_blocks = (file_size - data_start) // block_size + 2

        if first_block_num > 0:
            offset = data_start + (first_block_num - 1) * block_size
        else:
            offset = data_start

        blocks_visited = 0

        while offset < file_size and records_read < num_records and blocks_visited < max_blocks:
            # Read block header (6 bytes)
            f.seek(offset)
            block_header = f.read(6)
            if len(block_header) < 6:
                break

            next_block_num = struct.unpack('<H', block_header[0:2])[0]
            _prev_block = struct.unpack('<H', block_header[2:4])[0]
            last_rec_in_block = struct.unpack('<h', block_header[4:6])[0]

            blocks_visited += 1

            if last_rec_in_block < 0:
                # Empty/deleted block — follow linked list or stop
                if next_block_num > 0:
                    offset = data_start + (next_block_num - 1) * block_size
                else:
                    break
                continue

            num_recs_in_block = last_rec_in_block + 1
            rec_offset = offset + 6

            for _ in range(num_recs_in_block):
                if records_read >= num_records:
                    break

                f.seek(rec_offset)
                record_data = f.read(record_size)

                if len(record_data) < record_size:
                    break

                # Skip blank records (all zeros)
                if record_data == b'\x00' * record_size:
                    rec_offset += record_size
                    continue

                # Parse the record
                row = {}
                pos = 0
                for i, (ftype, fsize) in enumerate(fields):
                    raw = record_data[pos:pos + fsize]
                    name = field_names[i] if i < len(field_names) else f"FIELD_{i+1}"
                    row[name] = _decode_field(ftype, raw)
                    pos += fsize

                rows.append(row)
                rec_offset += record_size
                records_read += 1

            # Follow linked list to next block (NOT sequential increment)
            if next_block_num > 0:
                next_offset = data_start + (next_block_num - 1) * block_size
                if next_offset == offset:
                    break  # self-referencing, prevent infinite loop
                offset = next_offset
            else:
                break  # end of chain

        logger.info("%s: read %d/%d records", table_name, records_read, num_records)
        return rows


def _try_alt_field_layout(header: bytes, num_fields: int, record_size: int, table_name: str):
    """
    Try alternative field descriptor layout:
    - Types: 1-byte array at 0x78
    - Sizes: 2-byte LE array at 0x78 + num_fields
    """
    type_offset = 0x78
    size_offset = type_offset + num_fields
    needed = size_offset + num_fields * 2

    if needed > len(header):
        return None

    fields = []
    for i in range(num_fields):
        type_byte = header[type_offset + i]
        fsize = struct.unpack_from('<H', header, size_offset + i * 2)[0]
        field_type = FIELD_TYPES.get(type_byte, '?')
        fields.append((field_type, fsize))

    total = sum(s for _, s in fields)
    types_ok = all(t != '?' for t, _ in fields)
    sizes_ok = all(0 < s < 10000 for _, s in fields)

    if total == record_size and types_ok and sizes_ok:
        logger.info("%s: alternative field layout matched (1+2 byte)", table_name)
        return fields

    return None


def _extract_field_names_by_tablename(header: bytes, table_name: str, num_fields: int) -> list[str]:
    """
    Extract field names by finding the table name in the header
    and reading null-terminated strings after it.
    """
    # The table name appears in the header (without extension)
    # Field names follow after it
    markers = [
        table_name.upper().encode(INPUT_ENCODING),
        table_name.encode(INPUT_ENCODING),
        table_name.lower().encode(INPUT_ENCODING),
    ]

    for marker in markers:
        idx = header.find(marker)
        if idx < 0:
            continue

        # Skip past the marker and its null terminator
        names_start = idx + len(marker)
        # Skip any null bytes / padding after the marker
        while names_start < len(header) and header[names_start] == 0:
            names_start += 1

        names = _read_null_terminated_strings(header, names_start, num_fields)
        if len(names) == num_fields:
            return names

    return []


def _extract_field_names_by_scan(header: bytes, start: int, num_fields: int, limit: int) -> list[str]:
    """
    Scan the header for null-terminated ASCII strings starting at various offsets.
    """
    # Try a range of starting positions
    for offset in range(start, min(limit, len(header) - num_fields)):
        # Must start with a printable ASCII character
        if header[offset] < 0x20 or header[offset] > 0x7E:
            continue
        names = _read_null_terminated_strings(header, offset, num_fields)
        if len(names) == num_fields:
            return names

    return []


def _read_null_terminated_strings(data: bytes, start: int, count: int) -> list[str]:
    """Read `count` null-terminated strings from data starting at `start`."""
    names = []
    pos = start
    for _ in range(count):
        if pos >= len(data):
            break
        try:
            end = data.index(0x00, pos)
        except ValueError:
            break

        name = data[pos:end].decode('ascii', errors='replace').strip()
        # Validate: field names should be alphanumeric + underscores
        if not name or not all(c.isalnum() or c in '_' for c in name):
            break
        names.append(name)
        pos = end + 1

    return names
