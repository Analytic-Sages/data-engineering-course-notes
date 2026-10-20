"""Standalone reliable historical backfill: ERC-20 Transfer events -> GCS (Parquet).

Same four reliability mechanisms as the Postgres version, adapted for GCS:

  1. chunking          - range split into fixed-size block chunks
  2. checkpointing     - one small marker object per finished chunk in GCS;
                         re-runs skip chunks that already have a marker
  3. idempotent writes - deterministic object names, so re-running a chunk
                         overwrites the same files and never duplicates data
  4. retry + fallback  - RPC fallback list; oversized log spans split in half

Output layout (Hive-style, ready for a BigQuery external table):

    gs://<bucket>/raw/erc20_transfers/dt=2024-01-01/blocks_19000000_19002000.parquet

Checkpoints live OUTSIDE the data prefix so the external table never sees them:

    gs://<bucket>/_checkpoints/<pipeline>/19000000_19002000.json

BigQuery external table (on by default, --no-external-table to skip):
  Creates the dataset and a Hive-partitioned external table over the data prefix.
  It is created ONCE and is idempotent: if the table already exists it is left
  alone. New files and new dt= folders show up automatically on the next query,
  so the table never needs to be recreated when the pipeline uploads more data.

Authentication: Application Default Credentials (no auth code in this file).
    gcloud auth application-default login --no-launch-browser

You only need to tell it four things: bucket, dataset, first block, last block.
Anything you leave out is asked for in the terminal.

Usage:
    python historical_indexer_gcs.py
    python historical_indexer_gcs.py --bucket my-bucket --dataset raw \
        --from-block 19000000 --to-block 19002000

    # only upload to GCS, leave BigQuery alone
    python historical_indexer_gcs.py --bucket my-bucket --no-external-table

    # data already in GCS: only set up the BigQuery external table
    python historical_indexer_gcs.py --bucket my-bucket --dataset raw --skip-ingest
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from web3 import Web3
from web3.exceptions import LogTopicError, MismatchedABI

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("historical_indexer_gcs")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

RANGE_START  = 19_000_000
RANGE_END    = 19_020_000      # exclusive: the full 20,000-block reference range
CHUNK_SIZE   = 2_000           # blocks per checkpoint unit
DEMO_END     = RANGE_START + CHUNK_SIZE   # default for the guided run: 1 chunk, ~2,000 blocks
MAX_LOG_SPAN = 500             # max blocks per single eth_getLogs call

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

# uint256 does not fit in any Parquet integer type, so value_wei is stored as a
# decimal string. Cast in BigQuery: SAFE_CAST(value_wei AS BIGNUMERIC).
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
# Chunk / span helpers
# ---------------------------------------------------------------------------

def chunk_key(start: int, end: int) -> str:
    return f"{start}_{end}"


def all_chunks(start: int, end: int, size: int) -> list[tuple[int, int]]:
    """Split [start, end) into contiguous chunks of *size* blocks."""
    chunks, cursor = [], start
    while cursor < end:
        stop = min(cursor + size, end)
        chunks.append((cursor, stop))
        cursor = stop
    return chunks


def sub_spans(start: int, end: int, max_span: int = MAX_LOG_SPAN) -> list[tuple[int, int]]:
    """Split [start, end) into half-open sub-spans of at most *max_span* blocks."""
    spans, cursor = [], start
    while cursor < end:
        stop = min(cursor + max_span, end)
        spans.append((cursor, stop))
        cursor = stop
    return spans


# ---------------------------------------------------------------------------
# GCS storage + checkpoints
# ---------------------------------------------------------------------------

class GCSStore:
    def __init__(self, bucket_name: str, data_prefix: str, pipeline: str):
        self.client = storage.Client()          # credentials come from ADC
        self.bucket = self.client.bucket(bucket_name)
        self.data_prefix = data_prefix.strip("/")
        self.pipeline = pipeline

    # ---- data files

    def data_path(self, dt: str, start: int, end: int) -> str:
        # Deterministic name: re-running the same chunk overwrites the same object.
        return f"{self.data_prefix}/dt={dt}/blocks_{start}_{end}.parquet"

    def write_parquet(self, path: str, rows: list[dict]) -> None:
        table = pa.Table.from_pylist(rows, schema=PARQUET_SCHEMA)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        self.bucket.blob(path).upload_from_file(buf, content_type="application/octet-stream")

    # ---- checkpoints

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

    # ---- inspection helpers (used by the BigQuery step)

    def has_data(self) -> bool:
        """True if at least one Parquet file exists under the data prefix."""
        for blob in self.client.list_blobs(self.bucket, prefix=f"{self.data_prefix}/"):
            if blob.name.endswith(".parquet"):
                return True
        return False

    def bucket_location(self) -> str | None:
        """Bucket location (e.g. 'EU'), or None if we are not allowed to read it."""
        try:
            return self.client.get_bucket(self.bucket.name).location
        except Exception as exc:
            logger.warning("Could not read bucket location (%s)", exc)
            return None

    def partitions(self) -> list[str]:
        """Sorted dt values (YYYY-MM-DD) that currently exist under the data prefix."""
        found = set()
        for blob in self.client.list_blobs(self.bucket, prefix=f"{self.data_prefix}/"):
            for part in blob.name.split("/"):
                if part.startswith("dt="):
                    found.add(part[3:])
        return sorted(found)

    def ensure_bucket(self, location: str, create: bool | None) -> None:
        """Make sure the bucket exists, with a beginner-friendly message if it does not.

        create=True  -> create it without asking
        create=None  -> ask (only when running in an interactive terminal)
        create=False -> never create, just fail with instructions
        """
        from google.api_core.exceptions import Forbidden, NotFound

        name = self.bucket.name
        try:
            existing = self.client.get_bucket(name)
            print(f"  Bucket gs://{name} exists (location: {existing.location}).")
            return
        except NotFound:
            pass
        except Forbidden:
            # We may be allowed to read/write objects without being allowed to
            # read bucket metadata. Carry on; a real problem shows up on upload.
            print(f"  Could not read gs://{name} metadata (no permission) - assuming it exists.")
            return

        if create is None:
            answer = ask(f"  Bucket gs://{name} does not exist. Create it in {location}? (y/n)", "n")
            create = (answer or "n").lower().startswith("y")
        if not create:
            raise SystemExit(
                f"Bucket gs://{name} does not exist.\n"
                f"Create it yourself:\n"
                f"  gcloud storage buckets create gs://{name} --location={location}\n"
                f"or re-run with --create-bucket (optionally --location)."
            )

        bucket = self.client.bucket(name)
        bucket.iam_configuration.uniform_bucket_level_access_enabled = True
        self.client.create_bucket(bucket, location=location)
        print(f"  Created bucket gs://{name} in {location}.")


# ---------------------------------------------------------------------------
# BigQuery external table
# ---------------------------------------------------------------------------

class BigQueryExternalTable:
    """Creates (once) a Hive-partitioned external table over the GCS data prefix.

    The external table is only a definition: it points at
    gs://<bucket>/<prefix>/* and BigQuery reads whatever files exist at query
    time. Nothing here needs to run again when the pipeline uploads new files.
    """

    def __init__(self, store: GCSStore, dataset: str, table: str,
                 project: str | None = None, location: str | None = None,
                 require_partition_filter: bool = True):
        # Imported lazily so the ingestion script still runs without the
        # BigQuery package when --create-external-table is not used.
        from google.cloud import bigquery
        from google.api_core.exceptions import NotFound

        self._bigquery = bigquery
        self._NotFound = NotFound
        self.store = store
        self.client = bigquery.Client(project=project)   # credentials come from ADC
        self.project = self.client.project
        self.dataset = dataset
        self.table = table
        self.require_partition_filter = require_partition_filter

        # A dataset must live in the same location as the bucket it reads from.
        self.location = location or store.bucket_location()
        if not self.location:
            raise RuntimeError(
                "Could not determine the bucket location. "
                "Pass --bq-location (it must match the bucket location, e.g. EU or US)."
            )

    @property
    def dataset_id(self) -> str:
        return f"{self.project}.{self.dataset}"

    @property
    def table_id(self) -> str:
        return f"{self.project}.{self.dataset}.{self.table}"

    def ensure_dataset(self) -> None:
        try:
            ds = self.client.get_dataset(self.dataset_id)
        except self._NotFound:
            ds = self._bigquery.Dataset(self.dataset_id)
            ds.location = self.location
            self.client.create_dataset(ds)
            logger.info("Created dataset %s in %s", self.dataset_id, self.location)
            return
        if ds.location.upper() != self.location.upper():
            raise RuntimeError(
                f"Dataset {self.dataset_id} is in {ds.location} but the bucket is in "
                f"{self.location}. An external table needs both in the same location."
            )
        logger.info("Dataset %s already exists (%s)", self.dataset_id, ds.location)

    def ensure_table(self) -> bool:
        """Create the external table if missing. Returns True if it now exists."""
        try:
            self.client.get_table(self.table_id)
            logger.info("External table %s already exists - leaving it unchanged", self.table_id)
            return True
        except self._NotFound:
            pass

        # Schema and the dt partition column are read from the Parquet files,
        # so at least one file must exist before the table can be created.
        if not self.store.has_data():
            logger.warning(
                "No Parquet files under gs://%s/%s/ yet - skipping external table creation. "
                "Run the ingestion first, then run this script again.",
                self.store.bucket.name, self.store.data_prefix,
            )
            return False

        bq = self._bigquery
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
        logger.info("Created external table %s over %s/* (require_partition_filter=%s)",
                    self.table_id, base, self.require_partition_filter)
        return True

    def ensure(self) -> bool:
        self.ensure_dataset()
        return self.ensure_table()


# ---------------------------------------------------------------------------
# RPC helpers
# ---------------------------------------------------------------------------

class RPCIndexer:
    """Thin Web3 wrapper with automatic RPC-endpoint fallback."""

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
        logger.info("Switched to RPC endpoint: %s", self._endpoints[self._idx])

    # ---- block timestamps (used only to derive the dt= partition)

    def block_date(self, block_number: int) -> str:
        """UTC date (YYYY-MM-DD) of a block, cached."""
        if block_number in self._date_cache:
            return self._date_cache[block_number]
        for _ in range(len(self._endpoints) * 2):
            try:
                ts = self.w3.eth.get_block(block_number)["timestamp"]
                d = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                self._date_cache[block_number] = d
                return d
            except Exception as exc:
                logger.warning("get_block %d failed (%s) - rotating endpoint", block_number, exc)
                self._rotate()
                time.sleep(1)
        raise RuntimeError(f"All RPC endpoints failed for get_block({block_number})")

    def date_segments(self, first: int, last: int) -> list[tuple[int, int, str]]:
        """Split the inclusive block range [first, last] into (start, end, dt) segments.

        Block time is monotonic, so if the first and last block share a date,
        every block between them does too. Otherwise bisect. This costs only a
        handful of get_block calls instead of one per block.
        """
        d0, d1 = self.block_date(first), self.block_date(last)
        if d0 == d1:
            return [(first, last, d0)]
        mid = (first + last) // 2
        left = self.date_segments(first, mid)
        right = self.date_segments(mid + 1, last)
        # merge adjacent segments that landed on the same date
        if left[-1][2] == right[0][2]:
            merged = (left[-1][0], right[0][1], left[-1][2])
            return left[:-1] + [merged] + right[1:]
        return left + right

    # ---- logs

    def latest_block(self) -> int:
        for _ in range(len(self._endpoints) * 2):
            try:
                return self.w3.eth.get_block_number()
            except Exception as exc:
                logger.warning("get_block_number failed (%s) - rotating endpoint", exc)
                self._rotate()
                time.sleep(1)
        raise RuntimeError("All RPC endpoints failed on get_block_number")

    def fetch_logs(self, from_block: int, to_block: int) -> list:
        """eth_getLogs for all ERC-20 Transfer events in [from_block, to_block] (inclusive).

        Splits the span in half recursively if the provider rejects it as too
        large, until the span is <= 2 blocks.
        """
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
            logger.warning("Span %d..%d too large or failed - splitting in half",
                           from_block, to_block)
            mid = (from_block + to_block) // 2
            return self.fetch_logs(from_block, mid) + self.fetch_logs(mid + 1, to_block)
        raise RuntimeError(f"All RPC endpoints failed for span {from_block}..{to_block}")

    def decode_transfer(self, log) -> dict | None:
        """Decode a raw Transfer log. Returns None for non-standard logs (e.g. ERC-721)."""
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
# Pipeline
# ---------------------------------------------------------------------------

class HistoricalIndexer:
    """Chunked, resumable backfill from an EVM RPC into partitioned Parquet on GCS."""

    def __init__(self, store: GCSStore, rpc_url: str | None = None,
                 range_start: int = RANGE_START, range_end: int = RANGE_END,
                 chunk_size: int = CHUNK_SIZE):
        self.store = store
        self.range_start = range_start
        self.range_end = range_end
        self.chunk_size = chunk_size
        self.rpc = RPCIndexer(rpc_url)

    def run_chunk(self, start: int, end: int) -> tuple[int, list[str]]:
        """Fetch, decode, and write one chunk [start, end). Returns (rows, files)."""
        key = chunk_key(start, end)
        logger.info("chunk %s starting", key)
        transfers: dict[tuple[str, int], dict] = {}   # dedupe on (tx_hash, log_index)

        for s, e in sub_spans(start, end):
            # sub-spans are half-open [s, e) but eth_getLogs is inclusive: use e - 1,
            # otherwise the boundary block is fetched twice.
            # A failure here propagates on purpose: the chunk must NOT be
            # checkpointed if any part of it is missing.
            logs = self.rpc.fetch_logs(s, e - 1)
            for log in logs:
                decoded = self.rpc.decode_transfer(log)
                if decoded:
                    transfers[(decoded["tx_hash"], decoded["log_index"])] = decoded

        if not transfers:
            logger.info("chunk %s: no transfers found", key)
            return 0, []

        # Assign each row to its UTC date partition.
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
            logger.info("Nothing to do - all %d chunks already complete.", len(chunks))
            return

        logger.info("%d/%d chunks remain (starting at block %d)",
                    len(todo), len(chunks), todo[0][0])

        started = time.monotonic()
        for index, (s, e) in enumerate(todo, start=1):
            if max_chunks and index > max_chunks:
                logger.info("max-chunks=%d reached - stopping cleanly. Re-run to resume.", max_chunks)
                break
            try:
                n, files = self.run_chunk(s, e)
                # Marker is written LAST, only after all data files are uploaded.
                self.store.mark_done(chunk_key(s, e), n, files)
            except KeyboardInterrupt:
                logger.warning("Interrupted after %d/%d chunks - re-run to resume.",
                               index - 1, len(todo))
                raise SystemExit(130)

            elapsed = time.monotonic() - started
            remaining = (elapsed / index) * (len(todo) - index)
            logger.info("progress %d/%d | elapsed %.0fs | ETA ~%.0fs",
                        index, len(todo), elapsed, remaining)

        logger.info("Backfill complete.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def ask(prompt: str, default: str | None = None) -> str | None:
    """Ask in an interactive terminal; when not interactive, just return the default."""
    if not sys.stdin.isatty():
        return default
    suffix = f" [{default}]" if default not in (None, "") else ""
    answer = input(f"{prompt}{suffix}: ").strip()
    return answer or default


def ask_int(prompt: str, default: int) -> int:
    raw = ask(prompt, str(default))
    try:
        return int(str(raw).replace(",", "").replace("_", ""))
    except ValueError:
        raise SystemExit(f"'{raw}' is not a whole number.")


def step(number: int, total: int, title: str) -> None:
    print(f"\n=== Step {number}/{total}: {title} ===")


def main() -> None:
    p = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "Fetch ERC-20 Transfer events from Ethereum, upload them to Google Cloud Storage as\n"
            "Parquet, and expose them in BigQuery through an external table.\n\n"
            "Anything you leave out of the four main options is asked for interactively."
        ),
        epilog=(
            "example:\n"
            "  python historical_indexer_gcs.py --bucket my-bucket --dataset raw \\\n"
            "      --from-block 19000000 --to-block 19002000\n\n"
            "or just run it with no options and answer the questions."
        ),
    )

    core = p.add_argument_group("the four things to tell it")
    core.add_argument("--bucket", default=os.getenv("GCS_BUCKET"),
                      help="GCS bucket to upload into (no gs://)")
    core.add_argument("--dataset", "--bq-dataset", dest="dataset", default=os.getenv("BQ_DATASET"),
                      help="BigQuery dataset to create/use (letters, numbers, underscores)")
    core.add_argument("--from-block", type=int, default=None,
                      help=f"first block to ingest (default {RANGE_START:,})")
    core.add_argument("--to-block", type=int, default=None,
                      help=f"stop BEFORE this block, so it is not included (default {DEMO_END:,})")

    opt = p.add_argument_group("optional")
    opt.add_argument("--table", "--bq-table", dest="table", default=os.getenv("BQ_TABLE", "erc20_transfers_ext"),
                     help="external table name (default: erc20_transfers_ext)")
    opt.add_argument("--create-bucket", action="store_true",
                     help="create the bucket if it does not exist, without asking")
    opt.add_argument("--location", default=os.getenv("GCS_LOCATION", "EU"),
                     help="location used only when creating a new bucket (default: EU)")
    opt.add_argument("--no-external-table", action="store_true",
                     help="only upload to GCS; do not touch BigQuery")
    opt.add_argument("--skip-ingest", action="store_true",
                     help="do not fetch or upload anything; only set up the BigQuery external table")
    opt.add_argument("--reset", action="store_true",
                     help="forget which chunks are done, so they are fetched again (data files are kept)")

    adv = p.add_argument_group("advanced")
    adv.add_argument("--prefix", default=os.getenv("GCS_DATA_PREFIX", DEFAULT_DATA_PREFIX),
                     help=f"folder inside the bucket for the data (default: {DEFAULT_DATA_PREFIX})")
    adv.add_argument("--pipeline", default=PIPELINE_NAME, help="pipeline name (checkpoint folder)")
    adv.add_argument("--chunk-size", type=int, default=CHUNK_SIZE, help="blocks per checkpoint unit")
    adv.add_argument("--max-chunks", type=int, default=int(os.getenv("MAX_CHUNKS", "0")),
                     help="stop after N chunks (0 = no limit)")
    adv.add_argument("--rpc-url", default=os.getenv("WEB3_RPC_URL"))
    adv.add_argument("--bq-project", default=os.getenv("BQ_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT"),
                     help="BigQuery project (default: the ADC project)")
    adv.add_argument("--bq-location", default=os.getenv("BQ_LOCATION"),
                     help="dataset location (default: the bucket's location)")
    adv.add_argument("--no-require-partition-filter", action="store_true",
                     help="allow queries without a dt filter (they scan every file)")
    adv.add_argument("--create-external-table", action="store_true", help=argparse.SUPPRESS)  # now the default
    args = p.parse_args()

    # ---- gather the four inputs (ask for anything missing)
    bucket = (args.bucket or ask("GCS bucket name (no gs://)") or "").strip()
    if bucket.startswith("gs://"):
        bucket = bucket[5:]
    bucket = bucket.strip("/")
    if not bucket:
        p.error("a bucket name is required: pass --bucket NAME")

    use_bq = not args.no_external_table
    dataset = args.dataset
    if use_bq and not dataset:
        dataset = ask("BigQuery dataset name", "raw")
    if use_bq and not re.fullmatch(r"[A-Za-z0-9_]+", dataset or ""):
        p.error("dataset names may only contain letters, numbers and underscores")

    if args.skip_ingest:
        from_block, to_block = args.from_block or RANGE_START, args.to_block or DEMO_END
    else:
        from_block = args.from_block if args.from_block is not None else ask_int("First block to ingest", RANGE_START)
        to_block = args.to_block if args.to_block is not None else ask_int(
            "Stop before block (not included)", max(DEMO_END, from_block + args.chunk_size))
        if from_block >= to_block:
            p.error(f"--from-block ({from_block:,}) must be smaller than --to-block ({to_block:,})")

    if args.skip_ingest and not use_bq:
        p.error("--skip-ingest with --no-external-table leaves nothing to do")

    n_chunks = len(all_chunks(from_block, to_block, args.chunk_size))
    print("\nPlan")
    print(f"  Bucket   : gs://{bucket}/{args.prefix.strip('/')}/")
    if use_bq:
        print(f"  BigQuery : dataset '{dataset}', external table '{args.table}'")
    if not args.skip_ingest:
        print(f"  Blocks   : {from_block:,} up to (not including) {to_block:,}"
              f"  =  {to_block - from_block:,} blocks in {n_chunks} chunk(s) of {args.chunk_size:,}")

    total = (0 if args.skip_ingest else 1) + 1 + (1 if use_bq else 0)
    n = 0

    from google.api_core.exceptions import Forbidden
    from google.auth.exceptions import DefaultCredentialsError

    try:
        store = GCSStore(bucket, args.prefix, args.pipeline)

        n += 1
        step(n, total, "Check the GCS bucket")
        store.ensure_bucket(args.location, True if args.create_bucket else None)

        if not args.skip_ingest:
            n += 1
            step(n, total, "Fetch the blocks and upload Parquet files to GCS")
            if args.reset:
                removed = store.reset()
                print(f"  Reset {removed} checkpoint(s) for pipeline '{args.pipeline}'.")
            HistoricalIndexer(
                store=store,
                rpc_url=args.rpc_url,
                range_start=from_block,
                range_end=to_block,
                chunk_size=args.chunk_size,
            ).run(max_chunks=args.max_chunks)

        ext = None
        if use_bq:
            n += 1
            step(n, total, "Create the BigQuery dataset and external table (skipped if they exist)")
            ext = BigQueryExternalTable(
                store=store,
                dataset=dataset,
                table=args.table,
                project=args.bq_project,
                location=args.bq_location,
                require_partition_filter=not args.no_require_partition_filter,
            )
            ext.ensure()
    except DefaultCredentialsError:
        raise SystemExit(
            "\nNo Google credentials found. Python code needs its own login (separate from "
            "`gcloud auth login`):\n"
            "  gcloud auth application-default login --no-launch-browser\n"
            "then run this script again."
        )
    except Forbidden as exc:
        raise SystemExit(
            f"\nPermission denied: {getattr(exc, 'message', exc)}\n"
            "Check which account and project you are using with `gcloud config list`, and that "
            "it can write to the bucket and create BigQuery datasets/tables."
        )

    # ---- what just happened, and how to look at it
    base = f"gs://{bucket}/{args.prefix.strip('/')}"
    print("\nDone. Look at the result:")
    print(f"  gcloud storage ls -r {base}/")
    if ext is not None:
        days = store.partitions()
        if days:
            print("  bq query --use_legacy_sql=false \\")
            print(f"    'SELECT dt, COUNT(*) AS transfers FROM `{ext.table_id}` "
                  f"WHERE dt >= \"{days[0]}\" GROUP BY dt ORDER BY dt'")
        print("\nThe external table copies nothing: BigQuery reads the Parquet files in GCS each time "
              "you query.\nRe-running this script with more blocks adds files; the table sees them "
              "automatically.")


if __name__ == "__main__":
    main()