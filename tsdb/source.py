"""Access endpoint: ingests submissions and derives the versioned outgoing batch.

Each accepted submission is written atomically together with the derived
dimension table, the derived (re-hung) fact table and an immutable delta that
describes only what changed relative to the previous accepted version.  The
delta is exactly the payload the warehouse applies.

Storage layout (one SQLite file):

``source_state``   one row: current version
``submission``     every accepted submission id -> version + payload fingerprint
``decl``           current declaration records (mutable source state)
``fact``           current fact records (mutable source state)
``dim``            derived dimension rows ``[decl id, start, end, key, value]``
``factv``          derived fact rows ``[id, key, t, reading, s]``
``delta``          immutable per-version outgoing batch

Everything is written inside a single ``BEGIN IMMEDIATE`` transaction, so a
reader never observes a partially updated version and a crash leaves the
previous version intact.
"""

from __future__ import annotations

import argparse
import heapq
import json
import os
import sqlite3

from .common import (
    RequestError,
    canonical_json,
    fingerprint,
    parse_apply,
    parse_json_bytes,
    parse_submit,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS source_state (
    singleton  INTEGER PRIMARY KEY CHECK (singleton = 1),
    version    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS submission (
    sub_id     TEXT PRIMARY KEY,
    version    INTEGER NOT NULL,
    fp         TEXT NOT NULL
);

-- Mutable current source state (declarations).  ``p`` is stored as the
-- canonical JSON text of an arbitrary-precision integer so no artificial
-- range is imposed on the priority.
CREATE TABLE IF NOT EXISTS decl (
    id             TEXT PRIMARY KEY,
    key            TEXT NOT NULL,
    a              INTEGER NOT NULL,
    b              INTEGER NOT NULL,
    p_json         TEXT NOT NULL,
    value_json     TEXT NOT NULL
);

-- Mutable current source state (facts).  No segment yet: it is derived.
CREATE TABLE IF NOT EXISTS fact (
    id             TEXT PRIMARY KEY,
    key            TEXT NOT NULL,
    t              INTEGER NOT NULL,
    reading_json   TEXT NOT NULL
);

-- Derived dimension table; s = [decl_id, start, end].
CREATE TABLE IF NOT EXISTS dim (
    decl_id        TEXT NOT NULL,
    start          INTEGER NOT NULL,
    end            INTEGER NOT NULL,
    key            TEXT NOT NULL,
    value_json     TEXT NOT NULL,
    PRIMARY KEY (decl_id, start, end)
);

-- Derived fact table with its foreign key already resolved.
CREATE TABLE IF NOT EXISTS factv (
    id             TEXT PRIMARY KEY,
    key            TEXT NOT NULL,
    t              INTEGER NOT NULL,
    reading_json   TEXT NOT NULL,
    s_decl         TEXT,
    s_start        INTEGER,
    s_end          INTEGER
);

-- Outgoing batches.  Insert once; never updated or deleted.
CREATE TABLE IF NOT EXISTS delta (
    version        INTEGER PRIMARY KEY,
    sub_id         TEXT NOT NULL,
    payload        TEXT NOT NULL
);
"""


class Conflict(Exception):
    """A submission id reused with a different body."""

    def __init__(self, path: str = "$.id"):
        self.path = path
        super().__init__(path)


# ---------------------------------------------------------------------------
# Derivation
# ---------------------------------------------------------------------------


def _merge(pieces):
    """Merge adjacent runs attributed to the same declaration id."""
    merged = []
    for start, end, decl_id in pieces:
        if merged and merged[-1][1] == start and merged[-1][2] == decl_id:
            merged[-1] = (merged[-1][0], end, decl_id)
        else:
            merged.append((start, end, decl_id))
    return merged


def derive_dimensions(declarations):
    """Return ``{(decl_id, start, end): {"key", "value_json"}}``.

    For every key a sweep over boundaries tracks the covering declarations;
    at each point the winner is the largest ``p``, ties broken by the
    smallest record id in ASCII order.  Contiguous runs of the same winner
    are merged.  Points with no covering declaration produce no segment.
    """
    by_key = {}
    for rec in declarations:
        by_key.setdefault(rec["key"], []).append(rec)

    dimensions = {}
    for key in sorted(by_key):
        records = by_key[key]
        records_by_id = {rec["id"]: rec for rec in records}
        starts = {}
        ends = {}
        for rec in records:
            starts.setdefault(rec["a"], []).append(rec)
            ends.setdefault(rec["b"], []).append(rec)
        boundaries = sorted(set(starts) | set(ends))

        active = {}
        heap = []
        pieces = []
        for index in range(len(boundaries) - 1):
            point = boundaries[index]
            for rec in ends.get(point, ()):
                active.pop(rec["id"], None)
            for rec in starts.get(point, ()):
                active[rec["id"]] = rec
                heapq.heappush(heap, (-rec["p"], rec["id"]))
            while heap and heap[0][1] not in active:
                heapq.heappop(heap)
            if not heap:
                continue
            decl_id = heap[0][1]
            pieces.append((point, boundaries[index + 1], decl_id))
        for start, end, decl_id in _merge(pieces):
            winner = records_by_id[decl_id]
            dimensions[(decl_id, start, end)] = {
                "key": key,
                "value_json": winner["value_json"],
            }
    return dimensions


def _segment_for(segments, t: int):
    """Binary search a left-closed, right-open segment list sorted by start."""
    lo, hi = 0, len(segments)
    while lo < hi:
        mid = (lo + hi) // 2
        start, end, s = segments[mid]
        if t < start:
            hi = mid
        elif t >= end:
            lo = mid + 1
        else:
            return s
    return None


def derive_facts(facts, dimensions):
    """Re-hang each fact on its covering segment (or ``None``)."""
    segments_by_key = {}
    for (decl_id, start, end), rec in dimensions.items():
        segments_by_key.setdefault(rec["key"], []).append((start, end, [decl_id, start, end]))
    for key in segments_by_key:
        segments_by_key[key].sort()

    rows = {}
    for rec in facts:
        s = _segment_for(segments_by_key.get(rec["key"], ()), rec["t"])
        rows[rec["id"]] = {
            "id": rec["id"],
            "key": rec["key"],
            "t": rec["t"],
            "reading_json": rec["reading_json"],
            "s": s,
        }
    return rows


def _fact_tuple(rec):
    return (
        rec["key"],
        rec["t"],
        rec["reading_json"],
        None if rec["s"] is None else tuple(rec["s"]),
    )


def _dim_tuple(rec):
    return (rec["key"], rec["value_json"])


def compute_delta(version, old_dims, new_dims, old_facts, new_facts):
    """Net change between two derived states.

    Rows are ``[s, key, value]`` / ``[id, key, t, reading, s]``; deletes are
    primary keys.  Only rows whose content differs are included.
    """
    dim_rows = [
        [list(s), rec["key"], json.loads(rec["value_json"])]
        for s, rec in new_dims.items()
        if s not in old_dims or _dim_tuple(rec) != _dim_tuple(old_dims[s])
    ]
    dim_deletes = [list(s) for s in old_dims if s not in new_dims]
    dim_rows.sort(key=lambda row: row[0])
    dim_deletes.sort()

    fact_rows = [
        [
            record_id,
            rec["key"],
            rec["t"],
            json.loads(rec["reading_json"]),
            rec["s"],
        ]
        for record_id, rec in new_facts.items()
        if record_id not in old_facts or _fact_tuple(rec) != _fact_tuple(old_facts[record_id])
    ]
    fact_deletes = [record_id for record_id in old_facts if record_id not in new_facts]
    fact_rows.sort(key=lambda row: row[0])
    fact_deletes.sort()

    return {"v": version, "d": dim_rows, "dd": dim_deletes, "f": fact_rows, "fd": fact_deletes}


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


class Store:
    def __init__(self, path: str, timeout: float = 30.0):
        self.path = path
        self.timeout = timeout

    # -- connection -------------------------------------------------------

    def _connect(self):
        conn = sqlite3.connect(self.path, timeout=self.timeout, isolation_level=None)
        conn.execute("PRAGMA busy_timeout = %d" % int(self.timeout * 1000))
        conn.execute("PRAGMA synchronous = FULL")
        return conn

    def initialise(self):
        directory = os.path.dirname(os.path.abspath(self.path))
        if directory:
            os.makedirs(directory, exist_ok=True)
        conn = self._connect()
        try:
            # WAL is persistent in the database file; set it exactly once so
            # concurrent readers never contend on the pragma.
            conn.execute("PRAGMA journal_mode = WAL")
            conn.executescript(SCHEMA)
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO source_state (singleton, version) VALUES (1, 0)"
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()

    # -- loaders ----------------------------------------------------------

    @staticmethod
    def _load_dims(conn):
        return {
            (row[0], row[1], row[2]): {"key": row[3], "value_json": row[4]}
            for row in conn.execute(
                "SELECT decl_id, start, end, key, value_json FROM dim"
            )
        }

    @staticmethod
    def _load_facts(conn):
        return {
            row[0]: {
                "id": row[0],
                "key": row[1],
                "t": row[2],
                "reading_json": row[3],
                "s": None if row[4] is None else [row[4], row[5], row[6]],
            }
            for row in conn.execute(
                "SELECT id, key, t, reading_json, s_decl, s_start, s_end FROM factv"
            )
        }

    @staticmethod
    def _load_source(conn):
        decls = {
            row[0]: {
                "id": row[0],
                "key": row[1],
                "a": row[2],
                "b": row[3],
                "p": json.loads(row[4]),
                "p_json": row[4],
                "value_json": row[5],
            }
            for row in conn.execute(
                "SELECT id, key, a, b, p_json, value_json FROM decl"
            )
        }
        facts = {
            row[0]: {
                "id": row[0],
                "key": row[1],
                "t": row[2],
                "reading_json": row[3],
            }
            for row in conn.execute(
                "SELECT id, key, t, reading_json FROM fact"
            )
        }
        return decls, facts

    @staticmethod
    def _version(conn):
        return conn.execute(
            "SELECT version FROM source_state WHERE singleton = 1"
        ).fetchone()[0]

    # -- submit -----------------------------------------------------------

    def submit(self, body) -> dict:
        submission_id, ops = parse_submit(body)
        # "Same JSON structure": canonical_json sorts object keys, so object
        # key order and whitespace do not matter; array order and values do.
        body_fp = fingerprint(body)

        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                version, changed = self._submit_locked(conn, submission_id, ops, body_fp)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()

        return {"v": version}

    def _submit_locked(self, conn, submission_id, ops, body_fp):
        # Retries are judged first: same id, same body -> original version.
        row = conn.execute(
            "SELECT version, fp FROM submission WHERE sub_id = ?", (submission_id,)
        ).fetchone()
        if row is not None:
            if row[1] == body_fp:
                return int(row[0]), False
            raise Conflict()

        version = self._version(conn)
        previous_dims = self._load_dims(conn)
        previous_facts = self._load_facts(conn)
        decls, facts = self._load_source(conn)

        for category, record_id, new_row, path in ops:
            if category == "decl":
                target, other = decls, facts
            else:
                target, other = facts, decls
            current = target.get(record_id)
            if new_row is None:
                if current is None:
                    raise RequestError(path + "[1]")
                del target[record_id]
                continue
            if record_id in other:
                # One record id belongs to exactly one category.
                raise RequestError(path + "[0]")
            if category == "decl":
                target[record_id] = {
                    "id": record_id,
                    "key": new_row["key"],
                    "a": new_row["a"],
                    "b": new_row["b"],
                    "p": new_row["p"],
                    "p_json": canonical_json(new_row["p"]),
                    "value_json": canonical_json(new_row["value"]),
                }
            else:
                target[record_id] = {
                    "id": record_id,
                    "key": new_row["key"],
                    "t": new_row["t"],
                    "reading_json": canonical_json(new_row["reading"]),
                }

        new_dims = derive_dimensions(decls.values())
        new_facts = derive_facts(facts.values(), new_dims)
        new_version = version + 1
        delta = compute_delta(new_version, previous_dims, new_dims, previous_facts, new_facts)
        # Sanity check: what we emit must itself be an acceptable apply batch.
        parse_apply(delta)

        self._replace_source(conn, decls, facts)
        self._replace_derived(conn, new_dims, new_facts)
        conn.execute(
            "INSERT INTO delta (version, sub_id, payload) VALUES (?, ?, ?)",
            (new_version, submission_id, canonical_json(delta)),
        )
        conn.execute(
            "INSERT INTO submission (sub_id, version, fp) VALUES (?, ?, ?)",
            (submission_id, new_version, body_fp),
        )
        conn.execute("UPDATE source_state SET version = ? WHERE singleton = 1", (new_version,))
        return new_version, True

    @staticmethod
    def _replace_source(conn, decls, facts):
        conn.execute("DELETE FROM decl")
        conn.executemany(
            "INSERT INTO decl (id, key, a, b, p_json, value_json) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                (
                    rec["id"],
                    rec["key"],
                    rec["a"],
                    rec["b"],
                    rec["p_json"],
                    rec["value_json"],
                )
                for rec in (decls[key] for key in sorted(decls))
            ],
        )
        conn.execute("DELETE FROM fact")
        conn.executemany(
            "INSERT INTO fact (id, key, t, reading_json) "
            "VALUES (?, ?, ?, ?)",
            [
                (rec["id"], rec["key"], rec["t"], rec["reading_json"])
                for rec in (facts[key] for key in sorted(facts))
            ],
        )

    @staticmethod
    def _replace_derived(conn, dims, facts):
        conn.execute("DELETE FROM dim")
        conn.executemany(
            "INSERT INTO dim (decl_id, start, end, key, value_json) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                (s[0], s[1], s[2], rec["key"], rec["value_json"])
                for s, rec in sorted(dims.items())
            ],
        )
        conn.execute("DELETE FROM factv")
        conn.executemany(
            "INSERT INTO factv (id, key, t, reading_json, s_decl, s_start, s_end) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    record_id,
                    rec["key"],
                    rec["t"],
                    rec["reading_json"],
                    None if rec["s"] is None else rec["s"][0],
                    None if rec["s"] is None else rec["s"][1],
                    None if rec["s"] is None else rec["s"][2],
                )
                for record_id, rec in sorted(facts.items())
            ],
        )

    # -- snapshot / deltas ------------------------------------------------

    def snapshot(self) -> dict:
        """Return ``{v, d, f}``: the current dimension and fact tables."""
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            try:
                return self._snapshot(conn)
            finally:
                conn.execute("COMMIT")
        finally:
            conn.close()

    def _snapshot(self, conn):
        version = self._version(conn)
        dim_rows = [
            [[row[0], row[1], row[2]], row[3], json.loads(row[4])]
            for row in conn.execute(
                "SELECT decl_id, start, end, key, value_json FROM dim "
                "ORDER BY decl_id, start, end"
            )
        ]
        fact_rows = [
            [
                row[0],
                row[1],
                row[2],
                json.loads(row[3]),
                None if row[4] is None else [row[4], row[5], row[6]],
            ]
            for row in conn.execute(
                "SELECT id, key, t, reading_json, s_decl, s_start, s_end FROM factv "
                "ORDER BY id"
            )
        ]
        return {"v": version, "d": dim_rows, "f": fact_rows}

    def state(self) -> dict:
        """Debug view of the raw source records (not part of the contract)."""
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            try:
                version = self._version(conn)
                decls, facts = self._load_source(conn)
            finally:
                conn.execute("COMMIT")
        finally:
            conn.close()
        return {
            "v": version,
            "decl": [
                {
                    "id": rec["id"],
                    "key": rec["key"],
                    "a": rec["a"],
                    "b": rec["b"],
                    "p": rec["p"],
                    "value": json.loads(rec["value_json"]),
                }
                for _id, rec in sorted(decls.items())
            ],
            "fact": [
                {
                    "id": rec["id"],
                    "key": rec["key"],
                    "t": rec["t"],
                    "reading": json.loads(rec["reading_json"]),
                }
                for _id, rec in sorted(facts.items())
            ],
        }

    def delta(self, version: int):
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT payload FROM delta WHERE version = ?", (version,)
            ).fetchone()
            return None if row is None else json.loads(row[0])
        finally:
            conn.close()

    def deltas_after(self, version: int, limit: int = 256):
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT version, payload FROM delta WHERE version > ? "
                "ORDER BY version LIMIT ?",
                (version, limit),
            ).fetchall()
            return [(int(row[0]), json.loads(row[1])) for row in rows]
        finally:
            conn.close()

    def latest_version(self) -> int:
        conn = self._connect()
        try:
            return self._version(conn)
        finally:
            conn.close()

    # -- HTTP app ---------------------------------------------------------

    def app(self, method: str, path: str, query, raw: bytes):
        if method == "POST" and path == "/submit":
            body = parse_json_bytes(raw)
            try:
                return 200, self.submit(body)
            except Conflict as exc:
                return 409, {"error": exc.path}

        if method == "GET" and path == "/snapshot":
            return 200, self.snapshot()

        if method == "GET" and path == "/state":
            return 200, self.state()

        if method == "GET" and path == "/health":
            return 200, {"ok": True}

        if method == "GET" and path.startswith("/delta/"):
            tail = path[len("/delta/"):]
            if not tail.isdigit() or (len(tail) > 1 and tail[0] == "0"):
                raise RequestError("$")
            payload = self.delta(int(tail))
            if payload is None:
                return 404, {"error": "$"}
            return 200, payload

        if method == "GET" and path == "/deltas":
            raw_after = query.get("after", "0")
            if not raw_after.isdigit():
                raise RequestError("$")
            after = int(raw_after)
            batches = self.deltas_after(after)
            return 200, {
                "latest": self.latest_version(),
                "batches": [batch for _version, batch in batches],
            }

        return 404, {"error": "$"}


def main(argv=None):  # pragma: no cover - thin CLI
    from .http import serve

    parser = argparse.ArgumentParser(description="Temporal warehouse access endpoint")
    parser.add_argument("--db", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    store = Store(args.db)
    store.initialise()
    httpd = serve(store.app, args.host, args.port, args.verbose)
    print("access endpoint: http://%s:%d  db=%s" % (args.host, args.port, args.db), flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":  # pragma: no cover
    main()
