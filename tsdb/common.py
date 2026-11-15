"""Shared validation, canonicalisation and error helpers.

All protocol validation lives here so both HTTP services and the delivery
process agree on exactly what a valid payload looks like.  Errors carry a
JSONPath (``$``, ``$.d[0][1][2]``, ...) which is returned verbatim as
``{"error": path}``.
"""

from __future__ import annotations

import hashlib
import json
import re

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

#: Identifiers (keys, declaration ids, fact ids) are non-empty ASCII
#: alphanumeric strings.  ``fullmatch`` with ``\\Z`` avoids the trailing
#: newline quirk of ``$``.
_ID_RE = re.compile(r"[A-Za-z0-9]+\Z")


class RequestError(Exception):
    """A rejected request carrying the JSONPath of the offending value."""

    def __init__(self, path: str = "$"):
        self.path = path
        super().__init__(path)


# ---------------------------------------------------------------------------
# JSON
# ---------------------------------------------------------------------------


def _no_duplicate_pairs(pairs):
    obj = {}
    for key, value in pairs:
        if key in obj:
            raise ValueError("duplicate object key")
        obj[key] = value
    return obj


def _reject_constant(value):  # NaN / Infinity are not JSON
    raise ValueError("non-finite number")


def parse_json_bytes(raw: bytes):
    """Strictly parse UTF-8 JSON, rejecting NaN and duplicate object keys."""
    try:
        text = raw.decode("utf-8")
        return json.loads(
            text,
            object_pairs_hook=_no_duplicate_pairs,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, ValueError):
        # ValueError covers json.JSONDecodeError and the two hooks above.
        raise RequestError("$")


def canonical_json(value) -> str:
    """Deterministic JSON used for fingerprints, storage and wire payloads."""
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def fingerprint(value) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Scalar validators
# ---------------------------------------------------------------------------


def is_int(value) -> bool:
    """True for JSON integers only -- booleans are explicitly excluded."""
    return isinstance(value, int) and not isinstance(value, bool)


def check_identifier(value, path: str) -> str:
    if not isinstance(value, str) or _ID_RE.fullmatch(value) is None:
        raise RequestError(path)
    return value


def check_int64(value, path: str) -> int:
    if not is_int(value) or value < INT64_MIN or value > INT64_MAX:
        raise RequestError(path)
    return value


def check_integer(value, path: str) -> int:
    """Any JSON integer (booleans excluded); Python ints are unbounded."""
    if not is_int(value):
        raise RequestError(path)
    return value


def check_seconds(value, path: str) -> int:
    """Non-negative UTC integer seconds that fit SQLite's signed 64 bit int.

    ``reading`` is explicitly int64 by contract; timestamps use the same
    range so they can be stored and compared directly in SQLite.
    """
    if not is_int(value) or value < 0 or value > INT64_MAX:
        raise RequestError(path)
    return value


def check_reading(value, path: str):
    if value is None:
        return None
    return check_int64(value, path)


def check_value(value, path: str):
    if value is None:
        return None
    if not isinstance(value, str):
        raise RequestError(path)
    return value


def check_s(value, path: str):
    """Validate a segment primary key ``[declaration id, start, end]``."""
    if not isinstance(value, list) or len(value) != 3:
        raise RequestError(path)
    sid = check_identifier(value[0], path + "[0]")
    start = check_seconds(value[1], path + "[1]")
    end = check_seconds(value[2], path + "[2]")
    if start >= end:
        raise RequestError(path + "[2]")
    return (sid, start, end)


# ---------------------------------------------------------------------------
# submit
# ---------------------------------------------------------------------------


def check_no_extra_keys(body: dict, allowed, path: str = "$") -> None:
    """Reject unknown members so "same JSON structure" is well defined."""
    for key in body:
        if key in allowed:
            continue
        if _ID_RE.fullmatch(key) is not None:
            raise RequestError(path + "." + key)
        raise RequestError(path + "[" + json.dumps(key, ensure_ascii=True) + "]")


def extract_submit_id(body) -> str:
    if not isinstance(body, dict):
        raise RequestError("$")
    check_no_extra_keys(body, ("id", "d", "f"))
    if "id" not in body:
        raise RequestError("$.id")
    return check_identifier(body["id"], "$.id")


def _submit_decl_row(row, path: str):
    if not isinstance(row, list) or len(row) != 5:
        raise RequestError(path)
    key = check_identifier(row[0], path + "[0]")
    a = check_seconds(row[1], path + "[1]")
    b = check_seconds(row[2], path + "[2]")
    p = check_integer(row[3], path + "[3]")
    value = check_value(row[4], path + "[4]")
    if a >= b:
        raise RequestError(path + "[2]")
    return {"key": key, "a": a, "b": b, "p": p, "value": value}


def _submit_fact_row(row, path: str):
    if not isinstance(row, list) or len(row) != 3:
        raise RequestError(path)
    key = check_identifier(row[0], path + "[0]")
    t = check_seconds(row[1], path + "[1]")
    reading = check_reading(row[2], path + "[2]")
    return {"key": key, "t": t, "reading": reading}


def parse_submit(body):
    """Validate a submit body's shape and batch-local uniqueness.

    Database-dependent checks (does this id exist, in which category, is a
    deletion targeting a live record) are performed inside the store's write
    transaction.  This function only looks at the request itself.

    Returns ``(submission_id, ops)`` where each op is
    ``(category, record_id, row_or_None, entry_path)``.
    """
    submission_id = extract_submit_id(body)
    if "d" not in body:
        raise RequestError("$.d")
    if "f" not in body:
        raise RequestError("$.f")
    if not isinstance(body["d"], list):
        raise RequestError("$.d")
    if not isinstance(body["f"], list):
        raise RequestError("$.f")

    ops = []
    seen = {}

    for index, entry in enumerate(body["d"]):
        path = "$.d[%d]" % index
        if not isinstance(entry, list) or len(entry) != 2:
            raise RequestError(path)
        record_id = check_identifier(entry[0], path + "[0]")
        if record_id in seen:
            raise RequestError(path + "[0]")
        seen[record_id] = path
        row = entry[1]
        if row is not None:
            row = _submit_decl_row(row, path + "[1]")
        ops.append(("decl", record_id, row, path))

    for index, entry in enumerate(body["f"]):
        path = "$.f[%d]" % index
        if not isinstance(entry, list) or len(entry) != 2:
            raise RequestError(path)
        record_id = check_identifier(entry[0], path + "[0]")
        if record_id in seen:
            raise RequestError(path + "[0]")
        seen[record_id] = path
        row = entry[1]
        if row is not None:
            row = _submit_fact_row(row, path + "[1]")
        ops.append(("fact", record_id, row, path))

    return submission_id, ops


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------


def _apply_dim_row(row, path: str):
    if not isinstance(row, list) or len(row) != 3:
        raise RequestError(path)
    s = check_s(row[0], path + "[0]")
    key = check_identifier(row[1], path + "[1]")
    value = check_value(row[2], path + "[2]")
    return (s, key, value, path)


def _apply_fact_row(row, path: str):
    if not isinstance(row, list) or len(row) != 5:
        raise RequestError(path)
    record_id = check_identifier(row[0], path + "[0]")
    key = check_identifier(row[1], path + "[1]")
    t = check_seconds(row[2], path + "[2]")
    reading = check_reading(row[3], path + "[3]")
    s = None if row[4] is None else check_s(row[4], path + "[4]")
    return (record_id, key, t, reading, s, path)


def parse_apply(body):
    """Validate an apply payload's shape and payload-local uniqueness.

    Every parsed entry keeps the JSONPath it came from so referential errors
    can point at the exact offending value.
    """
    if not isinstance(body, dict):
        raise RequestError("$")
    check_no_extra_keys(body, ("v", "d", "dd", "f", "fd"))
    if "v" not in body:
        raise RequestError("$.v")
    version = check_int64(body["v"], "$.v")
    if version < 1:
        raise RequestError("$.v")
    for name in ("d", "dd", "f", "fd"):
        if name not in body:
            raise RequestError("$." + name)
        if not isinstance(body[name], list):
            raise RequestError("$." + name)

    dims = []
    seen_s = {}
    for index, row in enumerate(body["d"]):
        path = "$.d[%d]" % index
        parsed = _apply_dim_row(row, path)
        s = parsed[0]
        if s in seen_s:
            raise RequestError(path + "[0]")
        seen_s[s] = path
        dims.append(parsed)

    dim_deletes = []
    for index, raw_s in enumerate(body["dd"]):
        path = "$.dd[%d]" % index
        s = check_s(raw_s, path)
        if s in seen_s:
            raise RequestError(path)
        seen_s[s] = path
        dim_deletes.append((s, path))

    facts = []
    seen_id = {}
    for index, row in enumerate(body["f"]):
        path = "$.f[%d]" % index
        parsed = _apply_fact_row(row, path)
        record_id = parsed[0]
        if record_id in seen_id:
            raise RequestError(path + "[0]")
        seen_id[record_id] = path
        facts.append(parsed)

    fact_deletes = []
    for index, raw_id in enumerate(body["fd"]):
        path = "$.fd[%d]" % index
        record_id = check_identifier(raw_id, path)
        if record_id in seen_id:
            raise RequestError(path)
        seen_id[record_id] = path
        fact_deletes.append((record_id, path))

    return {
        "v": version,
        "d": dims,
        "dd": dim_deletes,
        "f": facts,
        "fd": fact_deletes,
    }
