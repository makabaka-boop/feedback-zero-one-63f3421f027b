#!/usr/bin/env python3
"""End-to-end tests: real HTTP access endpoint, delivery process, warehouse.

Runs both services on ephemeral ports in-process (threaded servers) and uses
the actual delivery client over HTTP.  Includes a hard-kill recovery test that
starts real subprocesses and ``os.kill(SIGKILL)``s them between operations.

    python3 -m tests.e2e            # from the repository root
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tsdb.delivery import Deliverer  # noqa: E402
from tsdb.http import serve  # noqa: E402
from tsdb.source import Store  # noqa: E402
from tsdb.warehouse import Warehouse  # noqa: E402


def free_port() -> int:
    sock = socket.socket()
    try:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
    finally:
        sock.close()


def request(url, payload=None, method=None):
    data = None
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, method=method or ("POST" if data else "GET"),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read()
        try:
            return exc.code, json.loads(body.decode("utf-8"))
        except Exception:
            return exc.code, body


def raw_post(url, data: bytes):
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


class Service:
    """A threaded HTTP service bound to an ephemeral port."""

    def __init__(self, app):
        self.port = free_port()
        self.httpd = serve(app, "127.0.0.1", self.port)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        self.wait_ready()
        return self

    def __exit__(self, *exc):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)

    def wait_ready(self):
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                request(self.url + "/health")
                return
            except Exception:
                time.sleep(0.02)
        raise RuntimeError("service did not start")

    @property
    def url(self):
        return "http://127.0.0.1:%d" % self.port


class Harness:
    def __init__(self, directory: str):
        self.directory = directory
        self.store = Store(os.path.join(directory, "source.db"))
        self.store.initialise()
        self.warehouse = Warehouse(os.path.join(directory, "warehouse.db"))
        self.warehouse.initialise()
        self.source_service = Service(self.store.app)
        self.warehouse_service = Service(self.warehouse.app)

    def __enter__(self):
        self.source_service.__enter__()
        self.warehouse_service.__enter__()
        self.deliverer = Deliverer(self.source_service.url, self.warehouse_service.url)
        return self

    def __exit__(self, *exc):
        self.warehouse_service.__exit__(*exc)
        self.source_service.__exit__(*exc)

    def submit(self, body):
        return request(self.source_service.url + "/submit", body)

    def apply(self, body):
        return request(self.warehouse_service.url + "/apply", body)

    def source_snapshot(self):
        return request(self.source_service.url + "/snapshot")[1]

    def warehouse_snapshot(self):
        return request(self.warehouse_service.url + "/snapshot")[1]

    def deliver(self):
        self.deliverer.run_once()
        return self.warehouse.version()


class BaseCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="tsdb-test-")
        self.harness = Harness(self.tmp)
        self.harness.__enter__()

    def tearDown(self):
        self.harness.__exit__(None, None, None)
        shutil.rmtree(self.tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# Derivation / submit
# ---------------------------------------------------------------------------


class TestExample(BaseCase):
    def test_readme_example_rehang_and_revert(self):
        h = self.harness
        status, body = h.submit(
            {"id": "s1", "d": [["d", ["k", 0, 20, 1, None]]], "f": [["f", ["k", 15, 7]]]}
        )
        self.assertEqual((status, body), (200, {"v": 1}))
        self.assertEqual(h.deliver(), 1)

        snap = h.warehouse_snapshot()
        self.assertEqual(snap["d"], [[["d", 0, 20], "k", None]])
        self.assertEqual(snap["f"], [["f", "k", 15, 7, ["d", 0, 20]]])

        # A higher priority declaration e carves out [10, 20).
        status, body = h.submit({"id": "s2", "d": [["e", ["k", 10, 20, 2, "x"]]], "f": []})
        self.assertEqual((status, body), (200, {"v": 2}))
        self.assertEqual(h.deliver(), 2)
        snap = h.warehouse_snapshot()
        self.assertEqual(
            snap["d"],
            [[["d", 0, 10], "k", None], [["e", 10, 20], "k", "x"]],
        )
        # The old fact is re-hung onto e's segment.
        self.assertEqual(snap["f"], [["f", "k", 15, 7, ["e", 10, 20]]])

        # Withdrawing e restores the original covering segment.
        status, body = h.submit({"id": "s3", "d": [["e", None]], "f": []})
        self.assertEqual((status, body), (200, {"v": 3}))
        self.assertEqual(h.deliver(), 3)
        snap = h.warehouse_snapshot()
        self.assertEqual(snap["d"], [[["d", 0, 20], "k", None]])
        self.assertEqual(snap["f"], [["f", "k", 15, 7, ["d", 0, 20]]])
        self.assertEqual(snap, h.source_snapshot())

    def test_no_covering_segment_leaves_null_fk(self):
        h = self.harness
        h.submit({"id": "s1", "d": [["d", ["k", 0, 10, 1, None]]], "f": [["f", ["k", 10, 3]]]})
        h.deliver()
        snap = h.warehouse_snapshot()
        self.assertEqual(snap["f"], [["f", "k", 10, 3, None]])
        self.assertEqual(snap["d"], [[["d", 0, 10], "k", None]])

    def test_priority_tie_uses_smallest_id(self):
        h = self.harness
        h.submit(
            {
                "id": "s1",
                "d": [
                    ["b", ["k", 0, 10, 5, "bb"]],
                    ["a", ["k", 0, 10, 5, "aa"]],
                ],
                "f": [],
            }
        )
        h.deliver()
        snap = h.warehouse_snapshot()
        self.assertEqual(snap["d"], [[["a", 0, 10], "k", "aa"]])

    def test_adjacent_equal_winner_runs_merge(self):
        h = self.harness
        h.submit(
            {
                "id": "s1",
                "d": [
                    ["a", ["k", 0, 10, 5, "x"]],
                    ["a2", ["k", 0, 10, 1, "y"]],
                    ["a3", ["k", 10, 20, 5, "x"]],
                ],
                "f": [],
            }
        )
        h.deliver()
        snap = h.warehouse_snapshot()
        # a wins [0,10); a3 wins [10,20).  Different declaration ids never
        # merge, even when key+value coincide.
        self.assertEqual(
            snap["d"],
            [
                [["a", 0, 10], "k", "x"],
                [["a3", 10, 20], "k", "x"],
            ],
        )

    def test_adjacent_runs_of_the_same_winner_merge(self):
        h = self.harness
        # a covers [0,20) and wins everywhere, so the fact that a lower
        # priority declaration exists in the middle must not split a's run.
        h.submit(
            {
                "id": "s1",
                "d": [
                    ["a", ["k", 0, 20, 9, "x"]],
                    ["b", ["k", 5, 10, 1, "y"]],
                ],
                "f": [],
            }
        )
        h.deliver()
        self.assertEqual(h.warehouse_snapshot()["d"], [[["a", 0, 20], "k", "x"]])

    def test_same_id_multi_interval_is_one_record(self):
        """A declaration record covers one interval; overlapping records of
        the same key but different ids each own only the spans they win."""
        h = self.harness
        h.submit(
            {
                "id": "s1",
                "d": [
                    ["a", ["k", 0, 10, 1, "x"]],
                    ["b", ["k", 0, 30, 2, "y"]],
                    ["c", ["k", 20, 30, 3, "z"]],
                ],
                "f": [["f", ["k", 25, 4]]],
            }
        )
        h.deliver()
        snap = h.warehouse_snapshot()
        self.assertEqual(
            snap["d"],
            [
                [["b", 0, 20], "k", "y"],
                [["c", 20, 30], "k", "z"],
            ],
        )
        self.assertEqual(snap["f"], [["f", "k", 25, 4, ["c", 20, 30]]])

    def test_gap_between_declarations_produces_no_segment(self):
        h = self.harness
        h.submit(
            {
                "id": "s1",
                "d": [["a", ["k", 0, 5, 1, "x"]], ["b", ["k", 10, 15, 1, "y"]]],
                "f": [["f1", ["k", 7, 1]], ["f2", ["k", 12, 2]]],
            }
        )
        h.deliver()
        snap = h.warehouse_snapshot()
        self.assertEqual(len(snap["d"]), 2)
        self.assertEqual(snap["f"][0], ["f1", "k", 7, 1, None])
        self.assertEqual(snap["f"][1], ["f2", "k", 12, 2, ["b", 10, 15]])

    def test_zero_reading_and_empty_string_value(self):
        h = self.harness
        h.submit({"id": "s1", "d": [["a", ["k", 0, 5, 0, ""]]], "f": [["f", ["k", 0, 0]]]})
        self.assertEqual(h.submit({"id": "s1b", "d": [], "f": []})[1], {"v": 2})
        self.assertEqual(h.deliver(), 2)
        snap = h.warehouse_snapshot()
        self.assertEqual(snap["d"], [[["a", 0, 5], "k", ""]])
        self.assertEqual(snap["f"], [["f", "k", 0, 0, ["a", 0, 5]]])

    def test_int64_boundaries_and_unbounded_priority(self):
        h = self.harness
        big = 2**80  # p is an integer, not restricted to int64
        h.submit(
            {
                "id": "s1",
                "d": [
                    ["lo", ["k", 0, 10, -(2**63), None]],
                    ["hi", ["k", 0, 10, 2**63 - 1, "x"]],
                ],
                "f": [["f", ["k", 9, -(2**63)]]],
            }
        )
        h.submit({"id": "s2", "d": [["top", ["k", 0, 10, big, "z"]]], "f": []})
        self.assertEqual(h.deliver(), 2)
        snap = h.warehouse_snapshot()
        self.assertEqual(snap["d"], [[["top", 0, 10], "k", "z"]])
        self.assertEqual(snap["f"], [["f", "k", 9, -(2**63), ["top", 0, 10]]])

    def test_out_of_int64_reading_and_time_rejected(self):
        h = self.harness
        status, body = h.submit(
            {"id": "s1", "d": [], "f": [["f", ["k", 1, 2**63]]]}
        )
        self.assertEqual((status, body), (400, {"error": "$.f[0][1][2]"}))
        status, body = h.submit(
            {"id": "s1", "d": [], "f": [["f", ["k", -1, 1]]]}
        )
        self.assertEqual((status, body), (400, {"error": "$.f[0][1][1]"}))
        status, body = h.submit(
            {"id": "s1", "d": [["a", ["k", 0, 2**63, 1, None]]], "f": []}
        )
        self.assertEqual((status, body), (400, {"error": "$.d[0][1][2]"}))
        self.assertEqual(h.source_snapshot()["v"], 0)


class TestSubmitValidation(BaseCase):
    def test_empty_batch_is_valid(self):
        status, body = self.harness.submit({"id": "e1", "d": [], "f": []})
        self.assertEqual((status, body), (200, {"v": 1}))

    def test_errors_are_json_paths_and_state_is_unchanged(self):
        h = self.harness
        cases = [
            ({"id": "s0", "d": [["x", ["k", 5, 5, 1, None]]], "f": []}, "$.d[0][1][2]"),
            ({"id": "s0", "d": [["x", ["k", 0, 5, True, None]]], "f": []}, "$.d[0][1][3]"),
            ({"id": "s0", "d": [["x", ["k", 0, 5, 1, 3]]], "f": []}, "$.d[0][1][4]"),
            ({"id": "s0", "d": [["x", ["k", 0, 5, 1, None]]], "f": [["f", ["k", -1, 3]]]}, "$.f[0][1][1]"),
            ({"id": "s0", "d": [["x", ["k", 0, 5, 1, None]]], "f": [["f", ["k", 1, True]]]}, "$.f[0][1][2]"),
            ({"id": "s0", "d": [["x", ["k", 0, 5, 1, None]], ["x", None]], "f": []}, "$.d[1][0]"),
            ({"id": "s0", "d": [["x", ["k", 0, 5, 1, None]]], "f": [["x", ["k", 1, 1]]]}, "$.f[0][0]"),
            ({"id": "s0", "d": [["x", None]], "f": []}, "$.d[0][1]"),
            ({"id": "bad id", "d": [], "f": []}, "$.id"),
            ({"id": "s0", "f": []}, "$.d"),
            ({"id": "s0", "d": [["x", ["k", 0, 5, 1, None, 1]]], "f": []}, "$.d[0][1]"),
            ({"id": "s0", "d": [["x", ["k", 0, 5, 1]]], "f": []}, "$.d[0][1]"),
            ({"id": "s0", "d": [], "f": [["f", ["k", 1]]]}, "$.f[0][1]"),
            ({"id": "s0", "d": [], "f": [], "extra": 1}, "$.extra"),
        ]
        for payload, expected in cases:
            with self.subTest(payload=payload):
                status, body = h.submit(payload)
                self.assertEqual((status, body), (400, {"error": expected}), payload)
                self.assertEqual(h.source_snapshot(), {"v": 0, "d": [], "f": []})

    def test_syntax_error_is_dollar(self):
        h = self.harness
        status, body = raw_post(h.source_service.url + "/submit", b"{not json")
        self.assertEqual((status, body), (400, {"error": "$"}))
        status, body = raw_post(h.source_service.url + "/submit", b"")
        self.assertEqual((status, body), (400, {"error": "$"}))
        self.assertEqual(h.source_snapshot(), {"v": 0, "d": [], "f": []})

    def test_duplicate_id_across_categories_rejected(self):
        h = self.harness
        status, body = h.submit({"id": "s1", "d": [["x", ["k", 0, 5, 1, None]]], "f": []})
        self.assertEqual(status, 200)
        status, body = h.submit({"id": "s2", "d": [], "f": [["x", ["k", 1, 1]]]})
        self.assertEqual((status, body), (400, {"error": "$.f[0][0]"}))

    def test_delete_of_missing_record_rejected(self):
        h = self.harness
        status, body = h.submit({"id": "s1", "d": [["ghost", None]], "f": []})
        self.assertEqual((status, body), (400, {"error": "$.d[0][1]"}))

    def test_update_in_place_rehangs_facts(self):
        h = self.harness
        h.submit({"id": "s1", "d": [["a", ["k", 0, 10, 1, "v1"]]], "f": [["f", ["k", 3, 9]]]})
        h.submit({"id": "s2", "d": [["a", ["k", 2, 10, 1, "v2"]]], "f": []})
        h.deliver()
        snap = h.warehouse_snapshot()
        self.assertEqual(snap["d"], [[["a", 2, 10], "k", "v2"]])
        self.assertEqual(snap["f"], [["f", "k", 3, 9, ["a", 2, 10]]])


class TestSubmitIdempotency(BaseCase):
    def test_same_body_same_version_different_body_conflict(self):
        h = self.harness
        payload = {"id": "s1", "d": [["a", ["k", 0, 5, 1, "v"]]], "f": []}
        self.assertEqual(h.submit(payload), (200, {"v": 1}))
        # Key order in the object does not matter: structure is compared.
        reordered = {"f": [], "d": [["a", ["k", 0, 5, 1, "v"]]], "id": "s1"}
        self.assertEqual(h.submit(reordered), (200, {"v": 1}))
        # Array order does matter: it is part of the JSON structure.
        shuffled = {"id": "s1", "d": [], "f": []}
        self.assertEqual(h.submit(shuffled), (409, {"error": "$.id"}))
        self.assertEqual(h.source_snapshot()["v"], 1)
        changed = {"id": "s1", "d": [["a", ["k", 0, 6, 1, "v"]]], "f": []}
        self.assertEqual(h.submit(changed), (409, {"error": "$.id"}))
        self.assertEqual(h.source_snapshot()["v"], 1)

    def test_retry_judged_before_validation(self):
        """A replay after the referenced records were deleted still returns the
        original version rather than an error."""
        h = self.harness
        first = {"id": "s1", "d": [["a", ["k", 0, 5, 1, "v"]]], "f": []}
        self.assertEqual(h.submit(first)[1], {"v": 1})
        self.assertEqual(h.submit({"id": "s2", "d": [["a", None]], "f": []})[1], {"v": 2})
        self.assertEqual(h.submit(first), (200, {"v": 1}))
        self.assertEqual(h.source_snapshot()["v"], 2)


class TestConcurrency(BaseCase):
    def test_concurrent_submissions_serialise(self):
        h = self.harness
        results = []
        lock = threading.Lock()

        def worker(index):
            payload = {"id": "c%d" % index, "d": [["a%d" % index, ["k%d" % index, 0, 10, 1, "v"]]], "f": []}
            status, body = h.submit(payload)
            with lock:
                results.append((status, body))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        versions = sorted(body["v"] for status, body in results if status == 200)
        self.assertEqual(versions, list(range(1, 13)))
        self.assertEqual(h.source_snapshot()["v"], 12)

    def test_concurrent_identical_retries_share_version(self):
        h = self.harness
        payload = {"id": "same", "d": [["a", ["k", 0, 5, 1, "v"]]], "f": []}
        results = []
        lock = threading.Lock()

        def worker():
            status, body = h.submit(payload)
            with lock:
                results.append((status, body))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(results, [(200, {"v": 1})] * 8)


# ---------------------------------------------------------------------------
# Warehouse apply
# ---------------------------------------------------------------------------


class TestApply(BaseCase):
    def test_idempotent_replay_and_conflicts(self):
        h = self.harness
        h.submit({"id": "s1", "d": [["a", ["k", 0, 5, 1, "v"]]], "f": [["f", ["k", 1, 1]]]})
        batch = h.source_service.url
        status, delta = request(batch + "/delta/1")
        self.assertEqual(status, 200)
        self.assertEqual(h.apply(delta), (200, {"v": 1}))
        # Same content, different key order in the nested primary key is a
        # different array, so it is rejected; a pure re-serialisation is not.
        self.assertEqual(h.apply(delta), (200, {"v": 1}))
        canonical = json.loads(json.dumps(delta))
        self.assertEqual(h.apply(canonical), (200, {"v": 1}))
        bad = json.loads(json.dumps(delta))
        bad["f"][0][3] = 99
        self.assertEqual(h.apply(bad), (409, {"error": "$.v"}))
        gap = json.loads(json.dumps(delta))
        gap["v"] = 5
        self.assertEqual(h.apply(gap), (409, {"error": "$.v"}))
        self.assertEqual(h.warehouse_snapshot()["v"], 1)

    def test_referential_failure_rejects_whole_batch(self):
        h = self.harness
        batch = {
            "v": 1,
            "d": [[["a", 0, 10], "k", "v"]],
            "dd": [],
            "f": [["f", "k", 5, 1, ["a", 0, 10]], ["g", "k", 6, 2, ["ghost", 0, 10]]],
            "fd": [],
        }
        self.assertEqual(h.apply(batch), (409, {"error": "$.f[1][4]"}))
        self.assertEqual(h.warehouse_snapshot(), {"v": 0, "d": [], "f": []})

    def test_delete_of_absent_row_rejected(self):
        h = self.harness
        batch = {"v": 1, "d": [], "dd": [["a", 0, 1]], "f": [], "fd": []}
        self.assertEqual(h.apply(batch), (409, {"error": "$.dd[0]"}))
        batch = {"v": 1, "d": [[["a", 0, 1], "k", None]], "dd": [], "f": [], "fd": ["nope"]}
        self.assertEqual(h.apply(batch), (409, {"error": "$.fd[0]"}))

    def test_payload_validation(self):
        cases = [
            ({"v": 1, "d": [], "dd": [], "f": [], "fd": []}, None),
            ({"v": True, "d": [], "dd": [], "f": [], "fd": []}, "$.v"),
            ({"v": 0, "d": [], "dd": [], "f": [], "fd": []}, "$.v"),
            ({"v": 1, "d": [], "dd": [], "f": []}, "$.fd"),
            ({"v": 1, "d": [[["a", 0, 1], "k", 1]], "dd": [], "f": [], "fd": []}, "$.d[0][2]"),
            ({"v": 1, "d": [[["a", 1, 1], "k", None]], "dd": [], "f": [], "fd": []}, "$.d[0][0][2]"),
            ({"v": 1, "d": [], "dd": [], "f": [["f", "k", 1, True, None]], "fd": []}, "$.f[0][3]"),
            ({"v": 1, "d": [], "dd": [], "f": [["f", "k", 1, 1, ["a", 0, 1]], ["f", "k", 2, 1, None]], "fd": []}, "$.f[1][0]"),
            ({"v": 1, "d": [[["a", 0, 1], "k", None]], "dd": [[["a", 0, 1]]], "f": [], "fd": []}, "$.dd[0]"),
        ]
        # Each case gets a pristine warehouse so "no state change" is exact.
        for payload, expected in cases:
            with self.subTest(payload=payload):
                tmp = tempfile.mkdtemp(prefix="tsdb-apply-")
                try:
                    warehouse = Warehouse(os.path.join(tmp, "warehouse.db"))
                    warehouse.initialise()
                    with Service(warehouse.app) as service:
                        status, body = request(service.url + "/apply", payload)
                        if expected is None:
                            self.assertEqual((status, body), (200, {"v": 1}))
                            self.assertEqual(warehouse.snapshot()["v"], 1)
                        else:
                            self.assertEqual((status, body), (400, {"error": expected}))
                            self.assertEqual(warehouse.snapshot(), {"v": 0, "d": [], "f": []})
                finally:
                    shutil.rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# Delivery
# ---------------------------------------------------------------------------


class TestDelivery(BaseCase):
    def test_delivers_every_version_in_order(self):
        h = self.harness
        for index in range(1, 6):
            h.submit({"id": "s%d" % index, "d": [["a", ["k", 0, index * 10, 1, "v%d" % index]]], "f": []})
        h.deliver()
        self.assertEqual(h.warehouse.version(), 5)
        self.assertEqual(h.source_snapshot(), h.warehouse_snapshot())

    def test_delta_contains_only_net_change(self):
        h = self.harness
        h.submit(
            {
                "id": "s1",
                "d": [["a", ["k", 0, 10, 1, "x"]], ["b", ["k", 10, 20, 1, "y"]]],
                "f": [["f1", ["k", 1, 1]], ["f2", ["k", 11, 2]]],
            }
        )
        # An unrelated update must not re-emit untouched rows.
        h.submit({"id": "s2", "d": [["b", ["k", 10, 20, 1, "y2"]]], "f": []})
        _status, delta = request(h.source_service.url + "/delta/2")
        self.assertEqual(delta["d"], [[["b", 10, 20], "k", "y2"]])
        self.assertEqual(delta["dd"], [])
        self.assertEqual(delta["f"], [])
        self.assertEqual(delta["fd"], [])

        # Shrinking a's interval deletes the old segment and re-hangs f1.
        h.submit({"id": "s3", "d": [["a", ["k", 0, 5, 1, "x"]]], "f": []})
        _status, delta = request(h.source_service.url + "/delta/3")
        self.assertEqual(delta["d"], [[["a", 0, 5], "k", "x"]])
        self.assertEqual(delta["dd"], [["a", 0, 10]])
        self.assertEqual(delta["f"], [["f1", "k", 1, 1, ["a", 0, 5]]])
        self.assertEqual(delta["fd"], [])
        # f2 was already on b and is untouched.
        self.assertNotIn("f2", [row[0] for row in delta["f"]])

    def test_lost_ack_resumes_without_duplicate_apply(self):
        h = self.harness
        h.submit({"id": "s1", "d": [["a", ["k", 0, 5, 1, "v"]]], "f": []})
        h.submit({"id": "s2", "d": [["a", ["k", 0, 6, 1, "v"]]], "f": []})
        # Apply version 1 directly, as if the acknowledgement was lost but
        # the write landed.  A second delivery must not rewrite it.
        _status, delta1 = request(h.source_service.url + "/delta/1")
        self.assertEqual(h.apply(delta1), (200, {"v": 1}))
        h.deliver()
        self.assertEqual(h.warehouse.version(), 2)
        self.assertEqual(h.source_snapshot(), h.warehouse_snapshot())

    def test_warehouse_downtime_is_retried(self):
        h = self.harness
        h.submit({"id": "s1", "d": [["a", ["k", 0, 5, 1, "v"]]], "f": []})
        port = h.warehouse_service.port
        h.warehouse_service.__exit__(None, None, None)  # warehouse is down

        holder = {}

        def restart_later():
            time.sleep(0.4)
            holder["httpd"] = serve(h.warehouse.app, "127.0.0.1", port)
            holder["thread"] = threading.Thread(
                target=holder["httpd"].serve_forever, daemon=True
            )
            holder["thread"].start()

        restarter = threading.Thread(target=restart_later, daemon=True)
        restarter.start()
        deliverer = Deliverer(
            h.source_service.url, "http://127.0.0.1:%d" % port,
            timeout=0.2, retry_delay=0.05, max_delay=0.1,
        )
        deliverer.run_once()
        restarter.join(timeout=5)
        self.assertEqual(h.warehouse.version(), 1)

        # Keep tearDown able to close a live service on that port.
        h.warehouse_service.httpd = holder["httpd"]
        h.warehouse_service.thread = holder["thread"]


# ---------------------------------------------------------------------------
# Hard-kill recovery with real processes
# ---------------------------------------------------------------------------


class TestHardKill(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="tsdb-kill-")
        self.procs = []

    def tearDown(self):
        for proc in self.procs:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=10)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _spawn(self, module, db, port):
        env = dict(os.environ)
        env["PYTHONPATH"] = ROOT
        proc = subprocess.Popen(
            [sys.executable, "-m", module, "--db", db, "--port", str(port)],
            cwd=ROOT, env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        )
        self.procs.append(proc)
        deadline = time.time() + 15
        while time.time() < deadline:
            if proc.poll() is not None:
                raise RuntimeError(proc.stderr.read().decode())
            try:
                with urllib.request.urlopen("http://127.0.0.1:%d/health" % port, timeout=1):
                    return proc
            except Exception:
                time.sleep(0.05)
        raise RuntimeError("service did not start")

    def _kill(self, proc):
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait(timeout=10)

    def test_sigkill_between_versions_keeps_consistency(self):
        source_db = os.path.join(self.tmp, "source.db")
        warehouse_db = os.path.join(self.tmp, "warehouse.db")
        source_port = free_port()
        warehouse_port = free_port()
        source = self._spawn("tsdb.source", source_db, source_port)
        self._spawn("tsdb.warehouse", warehouse_db, warehouse_port)
        base = "http://127.0.0.1:%d" % source_port

        self.assertEqual(request(base + "/submit", {"id": "s1", "d": [["a", ["k", 0, 10, 1, "v1"]]], "f": [["f", ["k", 5, 1]]]})[0], 200)
        self.assertEqual(request(base + "/submit", {"id": "s2", "d": [["a", ["k", 2, 10, 1, "v2"]]], "f": []})[0], 200)
        self.assertEqual(request(base + "/snapshot")[1]["v"], 2)

        self._kill(source)
        source = self._spawn("tsdb.source", source_db, source_port)
        snapshot = request(base + "/snapshot")[1]
        self.assertEqual(snapshot["v"], 2)
        self.assertEqual(snapshot["d"], [[["a", 2, 10], "k", "v2"]])
        self.assertEqual(snapshot["f"], [["f", "k", 5, 1, ["a", 2, 10]]])

        # Delivery survives being killed mid-stream and resumes.
        env = dict(os.environ)
        env["PYTHONPATH"] = ROOT
        deliverer = subprocess.Popen(
            [sys.executable, "-m", "tsdb.delivery",
             "--source", base,
             "--warehouse", "http://127.0.0.1:%d" % warehouse_port],
            cwd=ROOT, env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        )
        self.procs.append(deliverer)
        deadline = time.time() + 15
        warehouse = Warehouse(warehouse_db)
        while time.time() < deadline:
            if warehouse.version() >= 2:
                break
            time.sleep(0.05)
        self.assertEqual(warehouse.version(), 2)
        self._kill(deliverer)

        self.assertEqual(request(base + "/submit", {"id": "s3", "d": [["b", ["k", 4, 6, 9, "z"]]], "f": []})[0], 200)
        deliverer2 = subprocess.Popen(
            [sys.executable, "-m", "tsdb.delivery",
             "--source", base,
             "--warehouse", "http://127.0.0.1:%d" % warehouse_port, "--once"],
            cwd=ROOT, env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        )
        self.procs.append(deliverer2)
        self.assertEqual(deliverer2.wait(timeout=30), 0)
        self.assertEqual(warehouse.version(), 3)
        self.assertEqual(request(base + "/snapshot")[1], request("http://127.0.0.1:%d/snapshot" % warehouse_port)[1])

        # Kill the warehouse, restart, and verify a re-delivery is harmless.
        self._kill(self.procs[1])
        self._spawn("tsdb.warehouse", warehouse_db, warehouse_port)
        deliverer3 = subprocess.Popen(
            [sys.executable, "-m", "tsdb.delivery",
             "--source", base,
             "--warehouse", "http://127.0.0.1:%d" % warehouse_port, "--once"],
            cwd=ROOT, env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        )
        self.procs.append(deliverer3)
        self.assertEqual(deliverer3.wait(timeout=30), 0)
        self.assertEqual(request("http://127.0.0.1:%d/version" % warehouse_port)[1], {"v": 3})


if __name__ == "__main__":
    unittest.main(verbosity=2)
