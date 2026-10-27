"""fs-snmp ingest server.

Receives sample batches from the Go poller (POST /api/v1/samples) and writes
them to ClickHouse. Requests are only parsed and queued; a single background
writer merges queued batches into large ClickHouse inserts, which is what
ClickHouse wants (few big inserts, not many small ones). When the queue is
full the server answers 503 and the poller retries with backoff.
"""

import asyncio
import gzip
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import clickhouse_connect
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

log = logging.getLogger("ingest")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")

CH = dict(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
    username=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    database=os.getenv("CLICKHOUSE_DB", "snmp"),
)
TOKEN = os.getenv("INGEST_TOKEN", "")            # empty disables auth
FLUSH_ROWS = int(os.getenv("FLUSH_ROWS", "50000"))
FLUSH_SECONDS = float(os.getenv("FLUSH_SECONDS", "2"))
MAX_QUEUE = int(os.getenv("MAX_QUEUE_BATCHES", "500"))
MAX_BODY = int(os.getenv("MAX_BODY_BYTES", str(64 << 20)))

NUMERIC = {"integer", "counter32", "counter64", "gauge32", "timeticks"}
TEXT = {"string", "hex", "oid", "ipaddr"}
COLUMNS = ["ts", "device", "host", "grp", "metric", "idx", "value_type", "value"]


def split_samples(samples):
    """Convert wire samples into (metrics rows, info rows, skipped count)."""
    metrics, info, skipped = [], [], 0
    for s in samples:
        try:
            t = s["type"]
            v = s["value"]
            ts = datetime.fromtimestamp(s["ts"] / 1000.0, tz=timezone.utc)
            base = (ts, s["device"], s["host"], s["group"], s["name"],
                    s.get("index", ""), t)
        except (KeyError, TypeError, ValueError):
            skipped += 1
            continue
        if t in NUMERIC and v is not None:
            metrics.append(base + (float(v),))
        elif t in TEXT and v is not None:
            info.append(base + (str(v),))
        else:
            skipped += 1   # noSuchObject, endOfMibView, null, unknown types
    return metrics, info, skipped


class Writer:
    def __init__(self):
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_QUEUE)
        self.client = None
        self.inserted = 0
        self.last_insert = None
        self.last_error = None
        self._task = None

    def connect(self):
        delay = 1
        while True:
            try:
                self.client = clickhouse_connect.get_client(**CH)
                self.client.command("SELECT 1")
                log.info("connected to clickhouse %s:%s db=%s", CH["host"], CH["port"], CH["database"])
                return
            except Exception as e:  # noqa: BLE001
                log.warning("clickhouse not ready (%s), retrying in %ss", e, delay)
                time.sleep(delay)
                delay = min(delay * 2, 30)

    async def start(self):
        await asyncio.to_thread(self.connect)
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        # Drain whatever is still queued so a restart loses nothing.
        metrics, info = [], []
        while not self.queue.empty():
            m, i = self.queue.get_nowait()
            metrics += m
            info += i
        if metrics or info:
            await asyncio.to_thread(self._insert, metrics, info)

    async def _run(self):
        while True:
            metrics, info = [], []
            deadline = time.monotonic() + FLUSH_SECONDS
            # Block for the first batch, then merge whatever else arrives
            # until the row or time budget is reached.
            m, i = await self.queue.get()
            metrics += m
            info += i
            while len(metrics) + len(info) < FLUSH_ROWS:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    m, i = await asyncio.wait_for(self.queue.get(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                metrics += m
                info += i
            await asyncio.to_thread(self._insert, metrics, info)

    def _insert(self, metrics, info):
        delay = 1
        while True:
            try:
                if metrics:
                    self.client.insert("metrics", metrics, column_names=COLUMNS)
                if info:
                    self.client.insert("info", info, column_names=COLUMNS)
                self.inserted += len(metrics) + len(info)
                self.last_insert = time.time()
                self.last_error = None
                log.debug("inserted %d metrics, %d info rows", len(metrics), len(info))
                return
            except Exception as e:  # noqa: BLE001
                self.last_error = str(e)
                log.error("insert failed (%s), retrying in %ss", e, delay)
                time.sleep(delay)
                delay = min(delay * 2, 30)
                try:
                    self.client = clickhouse_connect.get_client(**CH)
                except Exception:  # noqa: BLE001
                    pass


writer = Writer()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await writer.start()
    yield
    await writer.stop()


app = FastAPI(title="fs-snmp ingest", lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    return {
        "ok": writer.last_error is None,
        "queued_batches": writer.queue.qsize(),
        "inserted_rows": writer.inserted,
        "last_insert": writer.last_insert,
        "last_error": writer.last_error,
    }


@app.post("/api/v1/samples")
async def samples(request: Request):
    if TOKEN and request.headers.get("authorization") != f"Bearer {TOKEN}":
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    body = await request.body()
    if len(body) > MAX_BODY:
        return JSONResponse({"error": "body too large"}, status_code=413)
    if request.headers.get("content-encoding", "").lower() == "gzip":
        try:
            body = gzip.decompress(body)
        except (OSError, EOFError):
            return JSONResponse({"error": "bad gzip body"}, status_code=400)
    try:
        data = json.loads(body)
    except ValueError:
        return JSONResponse({"error": "bad json"}, status_code=400)
    if not isinstance(data, list):
        return JSONResponse({"error": "expected a JSON array of samples"}, status_code=400)

    metrics, info, skipped = split_samples(data)
    if metrics or info:
        try:
            writer.queue.put_nowait((metrics, info))
        except asyncio.QueueFull:
            return JSONResponse({"error": "queue full, retry later"},
                                status_code=503, headers={"Retry-After": "5"})
    return {"accepted": len(metrics) + len(info), "skipped": skipped}
