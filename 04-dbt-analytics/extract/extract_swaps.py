"""Module 06 (extract) — Uniswap V3 Swap Extractor for PostgreSQL.

Pulls `Swap` event logs emitted by a Uniswap V3 pool over an arbitrary block
range via JSON-RPC (`eth_getLogs`), decodes the raw log data with web3.py, and
loads the decoded rows into PostgreSQL. Re-runs are idempotent: the natural
key `(tx_hash, log_index)` uses `ON CONFLICT DO NOTHING`.

The default pool (0x5777...2168) is the Uniswap V3 DAI/USDC 0.05% pool on
Ethereum mainnet, but `--pool` accepts any Uniswap V3 pool address.

Run from `docs/06-dbt-analytics/extract`:
    docker compose up -d
    uv sync
    uv run extract_swaps.py --from-block 19000000 --to-block 19000100
    uv run extract_swaps.py --from-block 19000000   # to-block = chain head
    uv run extract_swaps.py --from-block 19000000 --to-block 19000100 --csv swaps.csv
    uv run extract_swaps.py --from-block 19000000 --to-block 19000100 --dry-run
"""

from __future__ import annotations

import argparse
import csv
import logging
import math
import os
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from web3 import Web3

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("extract_swaps")

# ---------------------------------------------------------------------------
# Constants: the default pool and the canonical Uniswap V3 Swap event.
# ---------------------------------------------------------------------------

# Uniswap V3 DAI/USDC 0.05% pool (token0=DAI 18dec, token1=USDC 6dec).
DEFAULT_POOL = "0x5777d92f208679db4b9778590fa3cab3ac9e2168"

# event Swap(
#     address indexed sender,
#     address indexed recipient,
#     int256 amount0,
#     int256 amount1,
#     uint160 sqrtPriceX96,
#     uint128 liquidity,
#     int24 tick
# )
SWAP_EVENT_ABI = {
    "anonymous": False,
    "name": "Swap",
    "type": "event",
    "inputs": [
        {"name": "sender", "type": "address", "indexed": True},
        {"name": "recipient", "type": "address", "indexed": True},
        {"name": "amount0", "type": "int256", "indexed": False},
        {"name": "amount1", "type": "int256", "indexed": False},
        {"name": "sqrtPriceX96", "type": "uint160", "indexed": False},
        {"name": "liquidity", "type": "uint128", "indexed": False},
        {"name": "tick", "type": "int24", "indexed": False},
    ],
}

# Minimal ERC-20 view functions used to describe the pool's tokens.
ERC20_ABI = [
    {"name": "token0", "outputs": [{"type": "address"}], "inputs": [], "stateMutability": "view", "type": "function"},
    {"name": "token1", "outputs": [{"type": "address"}], "inputs": [], "stateMutability": "view", "type": "function"},
    {"name": "symbol", "outputs": [{"type": "string"}], "inputs": [], "stateMutability": "view", "type": "function"},
    {"name": "decimals", "outputs": [{"type": "uint8"}], "inputs": [], "stateMutability": "view", "type": "function"},
]

DEFAULT_CHUNK_SIZE = 2_000  # blocks per eth_getLogs call (Infura-safe window)
MAX_RETRIES = 5

# ---------------------------------------------------------------------------
# Schema + idempotent insert
# ---------------------------------------------------------------------------

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS pool_swaps (
    id               BIGSERIAL PRIMARY KEY,
    pool_address     TEXT NOT NULL,
    block_number     BIGINT NOT NULL,
    block_timestamp  TIMESTAMPTZ,
    tx_hash          TEXT NOT NULL,
    tx_index         INTEGER NOT NULL,
    log_index        INTEGER NOT NULL,
    sender           TEXT NOT NULL,
    recipient        TEXT NOT NULL,
    -- Raw token units (wei-scale). int256: negative = pool paid the user.
    amount0_raw      NUMERIC(78, 0) NOT NULL,
    amount1_raw      NUMERIC(78, 0) NOT NULL,
    -- Human-readable, decimals-adjusted (e.g. DAI 18dp / USDC 6dp).
    amount0          NUMERIC(78, 18) NOT NULL,
    amount1          NUMERIC(78, 18) NOT NULL,
    sqrt_price_x96   NUMERIC(78, 0) NOT NULL,
    liquidity        NUMERIC(78, 0) NOT NULL,
    tick             INTEGER NOT NULL,
    raw_data         TEXT NOT NULL,
    extracted_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tx_hash, log_index)
);

CREATE INDEX IF NOT EXISTS idx_pool_swaps_block_number ON pool_swaps (block_number);
CREATE INDEX IF NOT EXISTS idx_pool_swaps_pool ON pool_swaps (pool_address);
"""

_INSERT_SQL = """
INSERT INTO pool_swaps (
    pool_address, block_number, block_timestamp, tx_hash, tx_index, log_index,
    sender, recipient, amount0_raw, amount1_raw, amount0, amount1,
    sqrt_price_x96, liquidity, tick, raw_data
) VALUES (
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s
)
ON CONFLICT (tx_hash, log_index) DO NOTHING
"""


# ---------------------------------------------------------------------------
# RPC helpers
# ---------------------------------------------------------------------------

def connect(rpc_url: str) -> Web3:
    """Create a Web3 instance and verify the RPC endpoint answers."""
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 60}))
    if not w3.is_connected():
        logger.error("Cannot reach RPC endpoint at '%s'", rpc_url.split("/v3/")[0] + "/v3/***")
        sys.exit(1)
    logger.info("Connected to Ethereum mainnet — chain id %s, head block %s",
                w3.eth.chain_id, w3.eth.block_number)
    return w3


def fetch_logs_with_retry(w3: Web3, params: dict) -> list[dict]:
    """eth_getLogs with exponential backoff (Infura rate/limit errors)."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return w3.eth.get_logs(params)
        except Exception as exc:  # noqa: BLE001 — RPC raises various types
            if attempt == MAX_RETRIES:
                logger.error("eth_getLogs failed after %d attempts: %s", MAX_RETRIES, exc)
                raise
            wait = 2 ** attempt
            logger.warning("eth_getLogs error (%s), retry %d/%d in %ds",
                           exc, attempt, MAX_RETRIES, wait)
            time.sleep(wait)
    return []


def get_block_timestamps(w3: Web3, block_numbers: set[int]) -> dict[int, datetime]:
    """Fetch timestamps for a set of blocks, caching within the call."""
    timestamps: dict[int, datetime] = {}
    for i, number in enumerate(sorted(block_numbers), start=1):
        block = w3.eth.get_block(number)
        timestamps[number] = datetime.fromtimestamp(block["timestamp"], tz=timezone.utc)
        if i % 100 == 0:
            logger.info("  fetched timestamps %d/%d", i, len(block_numbers))
    return timestamps


# ---------------------------------------------------------------------------
# Decoding
# ---------------------------------------------------------------------------

def fetch_pool_metadata(w3: Web3, pool_address: str) -> dict:
    """Read token0/token1 addresses, symbols and decimals from the pool."""
    pool = w3.eth.contract(address=Web3.to_checksum_address(pool_address), abi=ERC20_ABI)
    token0_addr = pool.functions.token0().call()
    token1_addr = pool.functions.token1().call()

    def token_meta(addr: str) -> dict:
        t = w3.eth.contract(address=addr, abi=ERC20_ABI)
        return {"address": addr.lower(), "symbol": t.functions.symbol().call(),
                "decimals": t.functions.decimals().call()}

    meta = {"pool": pool_address.lower(),
            "token0": token_meta(token0_addr),
            "token1": token_meta(token1_addr)}
    logger.info("Pool %s = %s/%s (token0 %s, token1 %s)",
                pool_address.lower(), meta["token0"]["symbol"], meta["token1"]["symbol"],
                token0_addr, token1_addr)
    return meta


def raw_to_decimal(raw: int, decimals: int) -> str:
    """Scale a raw int256 token amount by decimals, return a plain decimal string."""
    sign = "-" if raw < 0 else ""
    raw = abs(raw)
    whole, frac = divmod(raw, 10 ** decimals)
    return f"{sign}{whole}.{frac:0{decimals}d}".rstrip("0").rstrip(".") or f"{sign}0"


def to_hex_str(value) -> str:
    """Normalize HexBytes / bytes / str into a lowercase 0x-prefixed hex string.

    web3 v6 returns HexBytes (``.hex()`` is 0x-prefixed) while plain ``bytes``
    is not — so normalize explicitly before comparing topic hashes.
    """
    if isinstance(value, str):
        hex_str = value
    elif hasattr(value, "hex"):
        hex_str = value.hex()
    else:
        hex_str = bytes(value).hex()
    return hex_str.lower() if hex_str.startswith("0x") else "0x" + hex_str.lower()


def decode_swap_log(log: dict, pool_meta: dict) -> dict | None:
    """Decode one raw Swap log into a flat dict. Returns None for non-Swap logs."""
    topics = [to_hex_str(t) for t in log["topics"]]
    swap_topic0 = to_hex_str(Web3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)"))
    if not topics or topics[0] != swap_topic0:
        return None

    # Indexed params come from topics, non-indexed from data — decode manually
    # so the script works on both web3 v6 (HexBytes) and v7 (bytes) log shapes.
    sender = "0x" + topics[1][-40:]
    recipient = "0x" + topics[2][-40:]
    data = bytes(log["data"])
    words = [int.from_bytes(data[i:i + 32], "big", signed=False) for i in range(0, len(data), 32)]
    amount0 = int.from_bytes(data[0:32], "big", signed=True)
    amount1 = int.from_bytes(data[32:64], "big", signed=True)
    sqrt_price_x96 = words[2]
    liquidity = words[3]
    # int24 tick is sign-extended into a 32-byte word; fold back from int256.
    tick_raw = words[4]
    tick = tick_raw - (1 << 256) if tick_raw >= (1 << 255) else tick_raw

    t0, t1 = pool_meta["token0"], pool_meta["token1"]
    return {
        "pool_address": pool_meta["pool"],
        "block_number": log["blockNumber"],
        "tx_hash": to_hex_str(log["transactionHash"]),
        "tx_index": log["transactionIndex"],
        "log_index": log["logIndex"],
        "sender": sender.lower(),
        "recipient": recipient.lower(),
        "amount0_raw": amount0,
        "amount1_raw": amount1,
        "amount0": raw_to_decimal(amount0, t0["decimals"]),
        "amount1": raw_to_decimal(amount1, t1["decimals"]),
        "sqrt_price_x96": sqrt_price_x96,
        "liquidity": liquidity,
        "tick": tick,
        "raw_data": bytes(log["data"]).hex(),
    }


# ---------------------------------------------------------------------------
# Extraction loop
# ---------------------------------------------------------------------------

def extract_swaps(w3: Web3, pool_meta: dict, from_block: int, to_block: int,
                  chunk_size: int) -> list[dict]:
    """Scan [from_block, to_block] in chunks and return decoded Swap rows."""
    swap_topic0 = Web3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)")
    total_chunks = math.ceil((to_block - from_block + 1) / chunk_size)
    rows: list[dict] = []

    for chunk_idx in range(total_chunks):
        start = from_block + chunk_idx * chunk_size
        end = min(start + chunk_size - 1, to_block)
        logger.info("Fetching logs %d/%d — blocks %d..%d", chunk_idx + 1, total_chunks, start, end)

        logs = fetch_logs_with_retry(w3, {
            "fromBlock": start,
            "toBlock": end,
            "address": Web3.to_checksum_address(pool_meta["pool"]),
            "topics": [swap_topic0],
        })

        for log in logs:
            decoded = decode_swap_log(log, pool_meta)
            if decoded:
                rows.append(decoded)
        logger.info("  %d Swap events so far", len(rows))

    return rows


def attach_timestamps(w3: Web3, rows: list[dict]) -> None:
    """Fill block_timestamp for each row (one eth_getBlock call per block)."""
    blocks = {r["block_number"] for r in rows}
    if not blocks:
        return
    logger.info("Fetching timestamps for %d distinct blocks...", len(blocks))
    timestamps = get_block_timestamps(w3, blocks)
    for row in rows:
        row["block_timestamp"] = timestamps.get(row["block_number"])


# ---------------------------------------------------------------------------
# Output: PostgreSQL + optional CSV
# ---------------------------------------------------------------------------

def get_db_url() -> str:
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    dbname = os.getenv("DB_NAME", "blockchain_db")
    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"


def load_to_postgres(rows: list[dict], db_url: str) -> int:
    """Create schema if needed and insert rows idempotently. Returns inserted count."""
    try:
        import psycopg
    except ImportError:
        logger.error("psycopg is not installed. Run: uv sync")
        sys.exit(1)

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(_CREATE_TABLE_SQL)
            inserted = 0
            for row in rows:
                cur.execute(_INSERT_SQL, (
                    row["pool_address"], row["block_number"], row.get("block_timestamp"),
                    row["tx_hash"], row["tx_index"], row["log_index"],
                    row["sender"], row["recipient"],
                    str(row["amount0_raw"]), str(row["amount1_raw"]),
                    row["amount0"], row["amount1"],
                    str(row["sqrt_price_x96"]), str(row["liquidity"]),
                    row["tick"], row["raw_data"],
                ))
                inserted += cur.rowcount
        conn.commit()
    return inserted


def save_csv(rows: list[dict], filepath: str) -> None:
    """Optionally dump decoded rows to CSV for inspection."""
    fields = ["pool_address", "block_number", "block_timestamp", "tx_hash", "tx_index",
              "log_index", "sender", "recipient", "amount0_raw", "amount1_raw",
              "amount0", "amount1", "sqrt_price_x96", "liquidity", "tick", "raw_data"]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logger.info("Wrote %d rows to %s", len(rows), filepath)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract and decode Uniswap V3 Swap events for any pool into PostgreSQL.")
    parser.add_argument("--from-block", type=int, required=True,
                        help="First block of the range (inclusive)")
    parser.add_argument("--to-block", type=int, default=None,
                        help="Last block of the range (inclusive). Defaults to the chain head.")
    parser.add_argument("--pool", default=DEFAULT_POOL,
                        help=f"Uniswap V3 pool address (default: {DEFAULT_POOL} = DAI/USDC 0.05%%)")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE,
                        help=f"Blocks per eth_getLogs call (default: {DEFAULT_CHUNK_SIZE})")
    parser.add_argument("--db-url", default=None,
                        help="PostgreSQL URL (default: built from DB_* env vars)")
    parser.add_argument("--csv", default=None, metavar="PATH",
                        help="Also write decoded rows to a CSV file")
    parser.add_argument("--dry-run", action="store_true",
                        help="Extract and decode but do not write to PostgreSQL")
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()

    rpc_url = os.getenv("WEB3_RPC_URL")
    if not rpc_url:
        logger.error("WEB3_RPC_URL is not set. Copy .env.example to .env and add your Infura URL.")
        sys.exit(1)

    w3 = connect(rpc_url)
    to_block = args.to_block if args.to_block is not None else w3.eth.block_number
    if args.from_block > to_block:
        logger.error("--from-block (%d) is after --to-block (%d)", args.from_block, to_block)
        sys.exit(1)

    pool_meta = fetch_pool_metadata(w3, args.pool)
    rows = extract_swaps(w3, pool_meta, args.from_block, to_block, args.chunk_size)
    logger.info("Decoded %d Swap events in blocks %d..%d", len(rows), args.from_block, to_block)

    if not rows:
        logger.info("No swaps found in this range — nothing to do.")
        return

    attach_timestamps(w3, rows)

    if args.csv:
        save_csv(rows, args.csv)

    if args.dry_run:
        logger.info("--dry-run: skipping PostgreSQL load. Sample row:")
        sample = dict(rows[0])
        sample.pop("raw_data", None)
        for k, v in sample.items():
            print(f"  {k}: {v}")
        return

    inserted = load_to_postgres(rows, args.db_url or get_db_url())
    skipped = len(rows) - inserted
    print(f"\n✅ Loaded {inserted} swaps into PostgreSQL "
          f"({skipped} duplicates skipped).")
    print("   Table: public.pool_swaps")


if __name__ == "__main__":
    main()
