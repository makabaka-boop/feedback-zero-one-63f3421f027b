"""Delivery process: drains the access endpoint's deltas into the warehouse.

Guarantees:

* strictly one version at a time, in ascending order;
* the warehouse is the source of truth for how far we got, so a lost
  acknowledgement or a restart simply resumes from ``warehouse.version + 1``;
* a batch is never re-sent once the warehouse reports it applied (``apply``
  is idempotent for the same content, but we avoid depending on that);
* retries use bounded exponential backoff and never advance the cursor.
"""

from __future__ import annotations

import argparse
import json
import threading
import time
import urllib.error
import urllib.request

from .common import canonical_json


class DeliveryError(Exception):
    pass


def _post(url: str, payload, timeout: float):
    data = canonical_json(payload).encode("utf-8")
    request = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read()
        try:
            return exc.code, json.loads(body.decode("utf-8"))
        except Exception:
            return exc.code, {"error": "$"}
    except (urllib.error.URLError, OSError, ValueError) as exc:
        raise DeliveryError("transport failure: %s" % (exc,))


def _get(url: str, timeout: float):
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read()
        try:
            return exc.code, json.loads(body.decode("utf-8"))
        except Exception:
            return exc.code, {"error": "$"}
    except (urllib.error.URLError, OSError, ValueError) as exc:
        raise DeliveryError("transport failure: %s" % (exc,))


class Deliverer:
    def __init__(self, source_url: str, warehouse_url: str, timeout: float = 10.0,
                 retry_delay: float = 0.2, max_delay: float = 5.0):
        self.source_url = source_url.rstrip("/")
        self.warehouse_url = warehouse_url.rstrip("/")
        self.timeout = timeout
        self.retry_delay = retry_delay
        self.max_delay = max_delay
        self.pause = 0.1
        self._stop = threading.Event()

    # -- primitives -------------------------------------------------------

    def warehouse_version(self) -> int:
        status, body = _get(self.warehouse_url + "/version", self.timeout)
        if status != 200 or not isinstance(body, dict) or "v" not in body:
            raise DeliveryError("cannot read warehouse version")
        return int(body["v"])

    def fetch(self, after: int):
        status, body = _get(
            self.source_url + "/deltas?after=%d" % after, self.timeout
        )
        if status != 200:
            raise DeliveryError("cannot read source deltas")
        return body

    def apply(self, batch):
        status, body = _post(self.warehouse_url + "/apply", batch, self.timeout)
        return status, body

    # -- one batch --------------------------------------------------------

    def deliver_batch(self, batch) -> bool:
        """Apply one batch, retrying transport failures indefinitely.

        Returns True once the warehouse acknowledges the batch.  A 409 is a
        hard stop: the warehouse either holds different content for this
        version or the version is not adjacent, which means the two sides
        diverged and retrying cannot help.
        """
        try:
            expected = int(batch["v"])
        except (KeyError, TypeError, ValueError):
            raise DeliveryError("malformed batch from source")
        delay = self.retry_delay
        while True:
            try:
                status, body = self.apply(batch)
            except DeliveryError:
                status, body = 0, {"error": "$"}
            if status == 200:
                if not isinstance(body, dict) or body.get("v") != expected:
                    raise DeliveryError(
                        "warehouse acknowledged batch %d unexpectedly: %r" % (expected, body)
                    )
                return True
            if 400 <= status < 500:
                # 409: the warehouse holds different content for this
                # version or the version is not adjacent.  Any other 4xx is
                # equally hopeless: retrying cannot change the answer.
                raise DeliveryError("warehouse rejected batch %d: %s" % (expected, body))
            time.sleep(delay)
            delay = min(delay * 2, self.max_delay)

    # -- loop -------------------------------------------------------------

    def run_once(self) -> int:
        """Deliver everything currently available.  Returns batches applied.

        Transport failures are retried (delivery must resume after a restart
        or a lost acknowledgement), so this only returns once the source has
        nothing left above the warehouse's version.  Batches are applied
        strictly in ascending order.
        """
        applied = 0
        backoff = self.retry_delay
        while not self._stop.is_set():
            try:
                current = self.warehouse_version()
                body = self.fetch(current)
            except DeliveryError:
                self._stop.wait(backoff)
                backoff = min(backoff * 2, self.max_delay)
                continue
            backoff = self.retry_delay
            batches = body.get("batches") or []
            if not batches:
                return applied
            progressed = False
            for batch in batches:
                version = int(batch["v"])
                if version <= current:
                    continue
                if version != current + 1:
                    break
                self.deliver_batch(batch)
                current = self.warehouse_version()
                applied += 1
                progressed = True
            if not progressed:
                return applied
        return applied

    def run_forever(self) -> None:
        backoff = self.retry_delay
        while not self._stop.is_set():
            try:
                applied = self.run_once()
                backoff = self.retry_delay
                if applied == 0:
                    self._stop.wait(self.pause)
            except (DeliveryError, OSError, json.JSONDecodeError, KeyError):
                self._stop.wait(backoff)
                backoff = min(backoff * 2, self.max_delay)

    def stop(self) -> None:
        self._stop.set()

    def watermark(self, version: int) -> bool:
        """True once the warehouse's version is at least ``version``."""
        return self.warehouse_version() >= version


def main(argv=None):  # pragma: no cover - thin CLI
    parser = argparse.ArgumentParser(description="Delta delivery process")
    parser.add_argument("--source", required=True, help="e.g. http://127.0.0.1:8080")
    parser.add_argument("--warehouse", required=True, help="e.g. http://127.0.0.1:8090")
    parser.add_argument("--once", action="store_true", help="drain once and exit")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--retry-delay", type=float, default=0.2)
    args = parser.parse_args(argv)

    deliverer = Deliverer(
        args.source, args.warehouse, timeout=args.timeout, retry_delay=args.retry_delay
    )
    if args.once:
        deliverer.run_once()
        return
    try:
        deliverer.run_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":  # pragma: no cover
    main()
