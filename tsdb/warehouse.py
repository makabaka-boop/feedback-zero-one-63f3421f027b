"""Applying batches to the warehouse (HTTP data warehouse side).

The warehouse owns two tables -- ``dim`` (segments) and ``fact`` -- and
applies one version at a time.  A batch is accepted only if, after applying
it, every fact's foreign key still resolves to an existing dimension row.
The check and the writes happen in one ``BEGIN IMMEDIATE`` transaction.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3

from .common import canonical_json, parse_apply

SCHEMA = """
CREATE TABLE IF NOT EXISTS warehouse_state (
    singleton  INTEGER PRIMARY KEY CHECK (singleton = 1),
    version    INTEGER NOT NULL
);

-- Applied batch payloads, kept verbatim (immutable once written).
CREATE TABLE IF NOT EXISTS applied (
    version    INTEGER PRIMARY KEY,
    payload    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim (
    decl_id        TEXT NOT NULL,
    start          INTEGER NOT NULL,
    end            INTEGER NOT NULL,
    key            TEXT NOT NULL,
    value_json     TEXT NOT NULL,
    PRIMARY KEY (decl_id, start, end)
);

CREATE TABLE IF NOT EXISTS fact (
    id             TEXT PRIMARY KEY,
    key            TEXT NOT NULL,
    t              INTEGER NOT NULL,
    reading_json   TEXT NOT NULL,
    s_decl         TEXT,
    s_start        INTEGER,
    s_end          INTEGER
);
"""


class Rejected(Exception):
    def __init__(self, path: str = "$"):
        self.path = path
        super().__init__(path)


def _canonical_batch(parsed) -> str:
    """Re-render a parsed batch so "same content" has one representation.

    Two payloads represent the same batch when they carry the same rows in
    the same array order with the same validated values; whitespace and
    object key order are irrelevant.
    """
    return canonical_json(
        {
            "v": parsed["v"],
            "d": [list(row) for row in parsed["d"]],
            "dd": [list(s) for s, _path in parsed["dd"]],
            "f": [list(row) for row in parsed["f"]],
            "fd": [record_id for record_id, _path in parsed["fd"]],
        }
    )


class Warehouse:
    def __init__(self, path: str, timeout: float = 30.0):
        self.path = path
        self.timeout = timeout

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
            conn.execute("PRAGMA journal_mode = WAL")
            conn.executescript(SCHEMA)
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO warehouse_state (singleton, version) VALUES (1, 0)"
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()

    def version(self) -> int:
        conn = self._connect()
        try:
            return conn.execute(
                "SELECT version FROM warehouse_state WHERE singleton = 1"
            ).fetchone()[0]
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # apply
    # ------------------------------------------------------------------

    def apply(self, body) -> dict:
        parsed = parse_apply(body)
        payload = _canonical_batch(parsed)

        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                result = self._apply_locked(conn, parsed, payload)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        return result

    def _apply_locked(self, conn, parsed, payload):
        current = conn.execute(
            "SELECT version FROM warehouse_state WHERE singleton = 1"
        ).fetchone()[0]
        version = parsed["v"]

        if version <= current:
            stored = conn.execute(
                "SELECT payload FROM applied WHERE version = ?", (version,)
            ).fetchone()
            if stored is None or stored[0] != payload:
                raise Rejected("$.v")
            return {"v": version}
        if version != current + 1:
            raise Rejected("$.v")

        dims, facts = self._state(conn)
        # Foreign keys are pointers into the *final* table, so deletes go
        # first and every check below sees the post-batch picture.
        for s, path in parsed["dd"]:
            if s not in dims:
                raise Rejected(path)
            del dims[s]
        for record_id, path in parsed["fd"]:
            if record_id not in facts:
                raise Rejected(path)
            del facts[record_id]
        for s, key, value, path in parsed["d"]:
            dims[s] = {"key": key, "value_json": canonical_json(value)}
        for record_id, key, t, reading, s, path in parsed["f"]:
            facts[record_id] = {
                "key": key,
                "t": t,
                "reading_json": canonical_json(reading),
                "s": s,
            }

        # Final referential integrity: every fact's s must name a live row.
        # Blame the dimension deletion if one removed the referenced row,
        # otherwise the fact row that pointed at a missing segment.
        dim_delete_paths = {s: path for s, path in parsed["dd"]}
        fact_paths = {row[0]: row[5] for row in parsed["f"]}
        for record_id, rec in facts.items():
            if rec["s"] is None:
                continue
            s = tuple(rec["s"])
            if s in dims:
                continue
            if s in dim_delete_paths:
                raise Rejected(dim_delete_paths[s])
            if record_id in fact_paths:
                raise Rejected(fact_paths[record_id] + "[4]")
            # A pre-existing fact points at a segment this batch removed or
            # never supplied; there is no single offending new row.
            raise Rejected("$.dd")

        self._write(conn, dims, facts)
        conn.execute(
            "INSERT INTO applied (version, payload) VALUES (?, ?)",
            (version, payload),
        )
        conn.execute(
            "UPDATE warehouse_state SET version = ? WHERE singleton = 1", (version,)
        )
        return {"v": version}

    @staticmethod
    def _state(conn):
        dims = {
            (row[0], row[1], row[2]): {"key": row[3], "value_json": row[4]}
            for row in conn.execute(
                "SELECT decl_id, start, end, key, value_json FROM dim"
            )
        }
        facts = {
            row[0]: {
                "key": row[1],
                "t": row[2],
                "reading_json": row[3],
                "s": None if row[4] is None else (row[4], row[5], row[6]),
            }
            for row in conn.execute(
                "SELECT id, key, t, reading_json, s_decl, s_start, s_end FROM fact"
            )
        }
        return dims, facts

    @staticmethod
    def _write(conn, dims, facts):
        conn.execute("DELETE FROM dim")
        conn.executemany(
            "INSERT INTO dim (decl_id, start, end, key, value_json) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                (s[0], s[1], s[2], rec["key"], rec["value_json"])
                for s, rec in sorted(dims.items())
            ],
        )
        conn.execute("DELETE FROM fact")
        conn.executemany(
            "INSERT INTO fact (id, key, t, reading_json, s_decl, s_start, s_end) "
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

    # ------------------------------------------------------------------
    # snapshot
    # ------------------------------------------------------------------

    def snapshot(self) -> dict:
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            try:
                version = conn.execute(
                    "SELECT version FROM warehouse_state WHERE singleton = 1"
                ).fetchone()[0]
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
                        "SELECT id, key, t, reading_json, s_decl, s_start, s_end FROM fact "
                        "ORDER BY id"
                    )
                ]
                return {"v": version, "d": dim_rows, "f": fact_rows}
            finally:
                conn.execute("COMMIT")
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def app(self, method: str, path: str, query, raw: bytes):
        from .common import parse_json_bytes

        if method == "POST" and path == "/apply":
            body = parse_json_bytes(raw)
            try:
                return 200, self.apply(body)
            except Rejected as exc:
                return 409, {"error": exc.path}

        if method == "GET" and path == "/snapshot":
            return 200, self.snapshot()

        if method == "GET" and path == "/version":
            return 200, {"v": self.version()}

        if method == "GET" and path == "/health":
            return 200, {"ok": True}

        return 404, {"error": "$"}


def main(argv=None):  # pragma: no cover - thin CLI
    from .http import serve

    parser = argparse.ArgumentParser(description="Temporal warehouse (HTTP)")
    parser.add_argument("--db", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    warehouse = Warehouse(args.db)
    warehouse.initialise()
    httpd = serve(warehouse.app, args.host, args.port, args.verbose)
    print("warehouse: http://%s:%d  db=%s" % (args.host, args.port, args.db), flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":  # pragma: no cover
    main()
