"""Assignment pipeline: three separate commands.

    1. extract   - fetch ERC-20 Transfers from RPC and upload Parquet to GCS
    2. external-table  - create BigQuery dataset + external table over GCS prefix
    3. query     - run a partition-filtered query against the external table

Usage:
    python pipeline.py extract --bucket blockchain_data --from-block 19000000 --to-block 19002000
    python pipeline.py external-table --bucket blockchain_data --dataset raw
    python pipeline.py query --bucket blockchain_data --dataset raw --dt 2024-01-01
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from web3 import Web3
from web3.exceptions import LogTopicError, MismatchedABI

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("assignment_pipeline")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

RANGE_START  = 19_000_000
CHUNK_SIZE   = 2_000
MAX_LOG_SPAN = 500

TRANSFER_TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

ERC20_MINIMAL_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True,  "name": "from",  "type": "address"},
            {"indexed": True,  "name": "to",    "type": "address"},
            {"indexed": False, "name": "value", "type": "uint256"},
        ],
        "name": "Transfer",
        "type": "event",
    }
]

PIPELINE_NAME      = "all_erc20_transfers_mainnet"
DEFAULT_DATA_PREFIX = "raw/erc20_transfers"
CHECKPOINT_PREFIX  = "_checkpoints"

PUBLIC_RPC_ENDPOINTS = [ep for ep in [
    os.getenv("WEB3_RPC_URL"),
    "https://eth.drpc.org",
    "https://1rpc.io/eth",
    "https://ethereum-rpc.publicnode.com",
    "https://eth.merkle.io",
] if ep]

PARQUET_SCHEMA = pa.schema([
    ("block_number",  pa.int64()),
    ("tx_hash",       pa.string()),
    ("log_index",     pa.int32()),
    ("token_address", pa.string()),
    ("from_address",  pa.string()),
    ("to_address",    pa.string()),
    ("value_wei",     pa.string()),
])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def chunk_key(start: int, end: int) -> str:
    return f"{start}_{end}"


def all_chunks(start: int, end: int, size: int) -> list[tuple[int, int]]:
    chunks, cursor = [], start
    while cursor < end:
        stop = min(cursor + size, end)
        chunks.append((cursor, stop))
        cursor = stop
    return chunks


def sub_spans(start: int, end: int, max_span: int = MAX_LOG_SPAN) -> list[tuple[int, int]]:
    spans, cursor = [], start
    while cursor < end:
        stop = min(cursor + max_span, end)
        spans.append((cursor, stop))
        cursor = stop
    return spans


# ---------------------------------------------------------------------------
# GCS storage
# ---------------------------------------------------------------------------

class GCSStore:
    def __init__(self, bucket_name: str, data_prefix: str, pipeline: str):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.data_prefix = data_prefix.strip("/")
        self.pipeline = pipeline

    def data_path(self, dt: str, start: int, end: int) -> str:
        return f"{self.data_prefix}/dt={dt}/blocks_{start}_{end}.parquet"

    def write_parquet(self, path: str, rows: list[dict]) -> None:
        table = pa.Table.from_pylist(rows, schema=PARQUET_SCHEMA)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        self.bucket.blob(path).upload_from_file(buf, content_type="application/octet-stream")

    def _cp_prefix(self) -> str:
        return f"{CHECKPOINT_PREFIX}/{self.pipeline}/"

    def done_chunks(self) -> set[str]:
        prefix = self._cp_prefix()
        keys = set()
        for blob in self.client.list_blobs(self.bucket, prefix=prefix):
            name = blob.name[len(prefix):]
            if name.endswith(".json"):
                keys.add(name[:-5])
        return keys

    def mark_done(self, key: str, records: int, files: list[str]) -> None:
        payload = {
            "pipeline": self.pipeline,
            "chunk": key,
            "records": records,
            "files": files,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        self.bucket.blob(f"{self._cp_prefix()}{key}.json").upload_from_string(
            json.dumps(payload), content_type="application/json"
        )

    def reset(self) -> int:
        blobs = list(self.client.list_blobs(self.bucket, prefix=self._cp_prefix()))
        for b in blobs:
            b.delete()
        return len(blobs)

    def has_data(self) -> bool:
        for blob in self.client.list_blobs(self.bucket, prefix=f"{self.data_prefix}/"):
            if blob.name.endswith(".parquet"):
                return True
        return False

    def bucket_location(self) -> str | None:
        try:
            return self.client.get_bucket(self.bucket.name).location
        except Exception as exc:
            logger.warning("Could not read bucket location (%s)", exc)
            return None

    def ensure_bucket(self, location: str, create: bool) -> None:
        from google.api_core.exceptions import Forbidden, NotFound
        name = self.bucket.name
        try:
            existing = self.client.get_bucket(name)
            print(f"  Bucket gs://{name} exists (location: {existing.location}).")
            return
        except NotFound:
            pass
        except Forbidden:
            print(f"  Could not read gs://{name} metadata - assuming it exists.")
            return

        if not create:
            raise SystemExit(
                f"Bucket gs://{name} does not exist.\n"
                f"Create it:\n  gcloud storage buckets create gs://{name} --location={location}"
            )

        bucket = self.client.bucket(name)
        bucket.iam_configuration.uniform_bucket_level_access_enabled = True
        self.client.create_bucket(bucket, location=location)
        print(f"  Created bucket gs://{name} in {location}.")


# ---------------------------------------------------------------------------
# BigQuery external table
# ---------------------------------------------------------------------------

class BigQueryExternalTable:
    def __init__(self, store: GCSStore, dataset: str, table: str,
                 project: str | None = None, location: str | None = None,
                 require_partition_filter: bool = True):
        self.store = store
        self.client = bigquery.Client(project=project)
        self.project = self.client.project
        self.dataset = dataset
        self.table = table
        self.require_partition_filter = require_partition_filter
        self.location = location or store.bucket_location()
        if not self.location:
            raise RuntimeError(
                "Could not determine bucket location. Pass --bq-location (must match bucket)."
            )

    @property
    def dataset_id(self) -> str:
        return f"{self.project}.{self.dataset}"

    @property
    def table_id(self) -> str:
        return f"{self.project}.{self.dataset}.{self.table}"

    def ensure_dataset(self) -> None:
        try:
            self.client.get_dataset(self.dataset_id)
        except NotFound:
            ds = bigquery.Dataset(self.dataset_id)
            ds.location = self.location
            self.client.create_dataset(ds)
            logger.info("Created dataset %s in %s", self.dataset_id, self.location)
            return
        logger.info("Dataset %s already exists", self.dataset_id)

    def ensure_table(self) -> bool:
        try:
            self.client.get_table(self.table_id)
            logger.info("External table %s already exists", self.table_id)
            return True
        except NotFound:
            pass

        if not self.store.has_data():
            logger.warning("No Parquet files yet - skipping external table creation.")
            return False

        bq = bigquery
        base = f"gs://{self.store.bucket.name}/{self.store.data_prefix}"

        hive = bq.HivePartitioningOptions()
        hive.mode = "AUTO"
        hive.source_uri_prefix = base
        hive.require_partition_filter = self.require_partition_filter

        cfg = bq.ExternalConfig("PARQUET")
        cfg.source_uris = [f"{base}/*"]
        cfg.autodetect = True
        cfg.hive_partitioning = hive

        table = bq.Table(self.table_id)
        table.external_data_configuration = cfg
        self.client.create_table(table)
        logger.info("Created external table %s over %s/*", self.table_id, base)
        return True

    def ensure(self) -> bool:
        self.ensure_dataset()
        return self.ensure_table()


# ---------------------------------------------------------------------------
# RPC indexer
# ---------------------------------------------------------------------------

class RPCIndexer:
    def __init__(self, rpc_url: str | None = None):
        endpoints = ([rpc_url] if rpc_url else []) + PUBLIC_RPC_ENDPOINTS
        seen, self._endpoints = set(), []
        for e in endpoints:
            if e and e not in seen:
                seen.add(e)
                self._endpoints.append(e)
        self._idx = 0
        self.w3 = self._connect(self._endpoints[0])
        self._date_cache: dict[int, str] = {}

    def _connect(self, url: str) -> Web3:
        return Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))

    def _rotate(self) -> None:
        self._idx = (self._idx + 1) % len(self._endpoints)
        self.w3 = self._connect(self._endpoints[self._idx])

    def block_date(self, block_number: int) -> str:
        if block_number in self._date_cache:
            return self._date_cache[block_number]
        for _ in range(len(self._endpoints) * 2):
            try:
                ts = self.w3.eth.get_block(block_number)["timestamp"]
                d = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                self._date_cache[block_number] = d
                return d
            except Exception as exc:
                logger.warning("get_block %d failed (%s) - rotating", block_number, exc)
                self._rotate()
                time.sleep(1)
        raise RuntimeError(f"All RPC endpoints failed for get_block({block_number})")

    def date_segments(self, first: int, last: int) -> list[tuple[int, int, str]]:
        d0, d1 = self.block_date(first), self.block_date(last)
        if d0 == d1:
            return [(first, last, d0)]
        mid = (first + last) // 2
        left = self.date_segments(first, mid)
        right = self.date_segments(mid + 1, last)
        if left[-1][2] == right[0][2]:
            merged = (left[-1][0], right[0][1], left[-1][2])
            return left[:-1] + [merged] + right[1:]
        return left + right

    def fetch_logs(self, from_block: int, to_block: int) -> list:
        try:
            for attempt in range(len(self._endpoints) * 2):
                try:
                    return self.w3.eth.get_logs({
                        "fromBlock": from_block,
                        "toBlock":   to_block,
                        "topics":    [TRANSFER_TOPIC0],
                    })
                except Exception as exc:
                    err = str(exc).lower()
                    if "limit" in err or "too large" in err or "-32016" in err:
                        raise
                    logger.warning("get_logs %d..%d attempt %d failed: %s",
                                   from_block, to_block, attempt + 1, exc)
                    self._rotate()
                    time.sleep(1.5)
        except Exception:
            if to_block - from_block <= 2:
                raise
            logger.warning("Span %d..%d too large - splitting", from_block, to_block)
            mid = (from_block + to_block) // 2
            return self.fetch_logs(from_block, mid) + self.fetch_logs(mid + 1, to_block)
        raise RuntimeError(f"All RPC endpoints failed for span {from_block}..{to_block}")

    def decode_transfer(self, log) -> dict | None:
        if len(log.get("topics", [])) != 3:
            return None
        try:
            contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(log["address"]),
                abi=ERC20_MINIMAL_ABI,
            )
            args = contract.events.Transfer().process_log(log)["args"]
            tx_hash = log["transactionHash"]
            if isinstance(tx_hash, bytes):
                tx_hash = tx_hash.hex()
            if not tx_hash.startswith("0x"):
                tx_hash = "0x" + tx_hash
            return {
                "block_number":  int(log["blockNumber"]),
                "tx_hash":       tx_hash.lower(),
                "log_index":     int(log["logIndex"]),
                "token_address": log["address"].lower(),
                "from_address":  args["from"].lower(),
                "to_address":    args["to"].lower(),
                "value_wei":     str(args["value"]),
            }
        except (LogTopicError, MismatchedABI, ValueError, KeyError):
            return None


# ---------------------------------------------------------------------------
# Historical indexer (extract only)
# ---------------------------------------------------------------------------

class HistoricalIndexer:
    def __init__(self, store: GCSStore, rpc_url: str | None = None,
                 range_start: int = RANGE_START, range_end: int = RANGE_START + CHUNK_SIZE,
                 chunk_size: int = CHUNK_SIZE):
        self.store = store
        self.range_start = range_start
        self.range_end = range_end
        self.chunk_size = chunk_size
        self.rpc = RPCIndexer(rpc_url)

    def run_chunk(self, start: int, end: int) -> tuple[int, list[str]]:
        key = chunk_key(start, end)
        logger.info("chunk %s starting", key)
        transfers: dict[tuple[str, int], dict] = {}

        for s, e in sub_spans(start, end):
            logs = self.rpc.fetch_logs(s, e - 1)
            for log in logs:
                decoded = self.rpc.decode_transfer(log)
                if decoded:
                    transfers[(decoded["tx_hash"], decoded["log_index"])] = decoded

        if not transfers:
            logger.info("chunk %s: no transfers found", key)
            return 0, []

        segments = self.rpc.date_segments(start, end - 1)
        by_dt: dict[str, list[dict]] = {}
        for row in transfers.values():
            for seg_start, seg_end, dt in segments:
                if seg_start <= row["block_number"] <= seg_end:
                    by_dt.setdefault(dt, []).append(row)
                    break

        files = []
        for dt, rows in sorted(by_dt.items()):
            rows.sort(key=lambda r: (r["block_number"], r["log_index"]))
            path = self.store.data_path(dt, start, end)
            self.store.write_parquet(path, rows)
            files.append(path)
            logger.info("chunk %s: wrote %d rows to gs://%s/%s",
                        key, len(rows), self.store.bucket.name, path)

        return len(transfers), files

    def run(self, max_chunks: int = 0) -> None:
        done = self.store.done_chunks()
        chunks = all_chunks(self.range_start, self.range_end, self.chunk_size)
        todo = [(s, e) for s, e in chunks if chunk_key(s, e) not in done]

        if not todo:
            logger.info("Nothing to do - all chunks already complete.")
            return

        logger.info("%d/%d chunks remain", len(todo), len(chunks))
        started = time.monotonic()
        for index, (s, e) in enumerate(todo, start=1):
            if max_chunks and index > max_chunks:
                logger.info("max-chunks reached - re-run to resume.")
                break
            try:
                n, files = self.run_chunk(s, e)
                self.store.mark_done(chunk_key(s, e), n, files)
            except KeyboardInterrupt:
                logger.warning("Interrupted after %d/%d chunks", index - 1, len(todo))
                raise SystemExit(130)

            elapsed = time.monotonic() - started
            remaining = (elapsed / index) * (len(todo) - index)
            logger.info("progress %d/%d | elapsed %.0fs | ETA ~%.0fs",
                        index, len(todo), elapsed, remaining)

        logger.info("Backfill complete.")


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_extract(args: argparse.Namespace) -> None:
    store = GCSStore(args.bucket, args.prefix, args.pipeline)
    store.ensure_bucket(args.location, args.create_bucket)

    if args.reset:
        removed = store.reset()
        print(f"  Reset {removed} checkpoint(s) for pipeline '{args.pipeline}'.")

    to_block = args.to_block if args.to_block is not None else args.from_block + args.chunk_size
    HistoricalIndexer(
        store=store,
        rpc_url=args.rpc_url,
        range_start=args.from_block,
        range_end=to_block,
        chunk_size=args.chunk_size,
    ).run(max_chunks=args.max_chunks)


def cmd_external_table(args: argparse.Namespace) -> None:
    store = GCSStore(args.bucket, args.prefix, args.pipeline)
    ext = BigQueryExternalTable(
        store=store,
        dataset=args.dataset,
        table=args.table,
        project=args.bq_project,
        location=args.bq_location,
        require_partition_filter=args.require_partition_filter,
    )
    ext.ensure()


def cmd_query(args: argparse.Namespace) -> None:
    client = bigquery.Client(project=args.bq_project)
    project = args.bq_project or client.project
    table_id = f"{project}.{args.dataset}.{args.table}"

    sql = (
        f"SELECT dt, COUNT(*) AS transfers "
        f"FROM `{table_id}` "
        f'WHERE dt = "{args.dt}" '
        f"GROUP BY dt ORDER BY dt"
    )
    print(f"Running query:\n{sql}\n")
    query_job = client.query(sql)
    rows = list(query_job.result())
    if not rows:
        print("No rows found.")
    for row in rows:
        print(row)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Assignment pipeline: extract, external table, query.")
    sub = p.add_subparsers(dest="command", required=True)

    # ---- extract ----
    pe = sub.add_parser("extract", help="Fetch from RPC and upload Parquet to GCS only.")
    pe.add_argument("--bucket", required=True, help="GCS bucket name (no gs://)")
    pe.add_argument("--from-block", type=int, default=RANGE_START)
    pe.add_argument("--to-block", type=int, default=None)
    pe.add_argument("--chunk-size", type=int, default=CHUNK_SIZE)
    pe.add_argument("--prefix", default=DEFAULT_DATA_PREFIX)
    pe.add_argument("--pipeline", default=PIPELINE_NAME)
    pe.add_argument("--location", default=os.getenv("GCS_LOCATION", "EU"))
    pe.add_argument("--create-bucket", action="store_true")
    pe.add_argument("--rpc-url", default=os.getenv("WEB3_RPC_URL"))
    pe.add_argument("--max-chunks", type=int, default=0)
    pe.add_argument("--reset", action="store_true")
    pe.set_defaults(func=cmd_extract)

    # ---- external-table ----
    pt = sub.add_parser("external-table", help="Create BigQuery dataset + external table over GCS.")
    pt.add_argument("--bucket", required=True, help="GCS bucket name (no gs://)")
    pt.add_argument("--dataset", required=True, help="BigQuery dataset name")
    pt.add_argument("--table", default="erc20_transfers_ext")
    pt.add_argument("--prefix", default=DEFAULT_DATA_PREFIX)
    pt.add_argument("--pipeline", default=PIPELINE_NAME)
    pt.add_argument("--bq-project", default=os.getenv("BQ_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT"))
    pt.add_argument("--bq-location", default=os.getenv("BQ_LOCATION", "EU"))
    pt.add_argument("--require-partition-filter", action="store_true", default=True)
    pt.add_argument("--no-require-partition-filter", action="store_false", dest="require_partition_filter")
    pt.set_defaults(func=cmd_external_table)

    # ---- query ----
    pq = sub.add_parser("query", help="Run a partition-filtered query against the external table.")
    pq.add_argument("--bucket", required=True, help="GCS bucket name (no gs://)")
    pq.add_argument("--dataset", required=True, help="BigQuery dataset name")
    pq.add_argument("--table", default="erc20_transfers_ext")
    pq.add_argument("--dt", required=True, help="Partition date (YYYY-MM-DD)")
    pq.add_argument("--bq-project", default=os.getenv("BQ_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT"))
    pq.set_defaults(func=cmd_query)

    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()