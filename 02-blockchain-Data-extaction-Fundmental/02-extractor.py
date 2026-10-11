"""Week 2 extractor: fetch Ethereum blocks and turn them into clean records.

This module is self-contained on purpose. It depends on exactly one other
file in this folder (config.py, for the RPC URL) and on httpx. Everything
else - the request, the hex-to-int conversion, the record shape - lives
here, so you can read the whole extraction flow top to bottom.

Output is transactions-only parquet, one row per transaction (flat, no
nesting). The file is written on every run - no format flag needed:

    uv run extractor.py --block 19000000
    # -> transactions_19000000.parquet

    uv run extractor.py --start 19000000 --end 19000019
    # -> transactions_19000000_19000019.parquet

"""

import argparse
from datetime import datetime, timezone

import httpx

from config import RPC_URL


def rpc_call(method: str, params: list) -> dict:
    """One JSON-RPC request. Returns the ``result`` field or raises."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    response = httpx.post(RPC_URL, json=payload, timeout=30)
    body = response.json()
    if "error" in body:
        raise RuntimeError(
            f"JSON-RPC error {body['error'].get('code')} on {method}: "
            f"{body['error'].get('message')}"
        )
    return body["result"]


def to_int(hex_value: str | None) -> int | None:
    """Hex string -> int. None and '' (contract creations) stay None."""
    if hex_value in (None, ""):
        return None
    return int(hex_value, 16)


def normalize_tx(tx: dict) -> dict:
    """Raw transaction from the node -> typed record. Value stays in wei."""
    return {
        "hash": tx["hash"],
        "block_number": to_int(tx.get("blockNumber")),
        "from_address": tx.get("from"),
        "to_address": tx.get("to"),  # None == contract creation
        "value_wei": to_int(tx.get("value")),
        "gas": to_int(tx.get("gas")),
        "gas_price_wei": to_int(tx.get("gasPrice")),
        "nonce": to_int(tx.get("nonce")),
        "input_byte_length": (len(tx.get("input", "0x")) - 2) // 2,
    }


def normalize_block(block: dict) -> dict:
    """Raw block from the node -> typed record, with normalized transactions."""
    return {
        "number": to_int(block["number"]),
        "hash": block["hash"],
        "timestamp": to_int(block.get("timestamp")),  # unix seconds
        "gas_used": to_int(block.get("gasUsed")),
        "gas_limit": to_int(block.get("gasLimit")),
        "base_fee_per_gas_wei": to_int(block.get("baseFeePerGas")),
        "transactions": [normalize_tx(tx) for tx in block.get("transactions", [])],
    }


def fetch_block(number: int) -> dict:
    """Fetch one block with full transactions and return it normalized."""
    raw = rpc_call("eth_getBlockByNumber", [hex(number), True])
    if raw is None:
        raise SystemExit(f"block {number} not found")
    return normalize_block(raw)


def blocks_to_tx_rows(blocks: list) -> list:
    """Flatten normalized blocks -> flat transaction rows (one row per tx).

    """
    tx_rows: list = []
    for block in blocks:
        tx_rows.extend(block.get("transactions", []))
    return tx_rows


def save_transactions_parquet(tx_rows: list, path: str) -> None:
    """Write flat tx rows to a parquet file (one row per transaction).

    """
    import pandas as pd

    df = pd.DataFrame(
        tx_rows,
        columns=[
            "hash",
            "block_number",
            "from_address",
            "to_address",
            "value_wei",
            "gas",
            "gas_price_wei",
            "nonce",
            "input_byte_length",
        ],
    )
    for col in ("value_wei", "gas_price_wei"):
        if col in df.columns:
            df[col] = df[col].map(lambda x: None if x is None else str(x))
    df.to_parquet(path, index=False)


def resolve_block_numbers(block, start, end, latest) -> list:
    
    if block is not None:
        if start is not None or end is not None:
            raise SystemExit("use either --block or --start/--end, not both")
        return [block]
    if start is not None or end is not None:
        if start is None or end is None:
            raise SystemExit("both --start and --end are required for a range")
        if start > end:
            raise SystemExit(f"--start ({start}) must be <= --end ({end})")
        return list(range(start, end + 1))
    return list(range(latest + 1, latest + 1))


def default_parquet_path(block_numbers: list) -> str:
    
    if len(block_numbers) == 1:
        return f"transactions_{block_numbers[0]}.parquet"
    return f"transactions_{block_numbers[0]}_{block_numbers[-1]}.parquet"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch Ethereum blocks, save transactions as parquet"
    )
    parser.add_argument(
        "--block", type=int,
        help="fetch one specific block",
    )
    parser.add_argument(
        "--start", type=int,
        help="range start block (inclusive, requires --end)",
    )
    parser.add_argument(
        "--end", type=int,
        help="range end block (inclusive, requires --start)",
    )
    
    args = parser.parse_args()

    if args.block is None and args.start is None and args.end is None:
        latest = int(rpc_call("eth_blockNumber", []), 16)
    else:
        latest = 0 
        
    block_numbers = resolve_block_numbers(
        args.block, args.start, args.end, latest
    )
    print('Fetching blocks: From ', block_numbers[0], ' to ', block_numbers[-1])
    blocks = [fetch_block(n) for n in block_numbers]

    out = default_parquet_path(block_numbers)
    tx_rows = blocks_to_tx_rows(blocks)
    save_transactions_parquet(tx_rows, out)
    print(f"wrote {len(tx_rows)} transactions from {len(blocks)} blocks to {out}")

    for block in blocks:
        ts = datetime.fromtimestamp(block["timestamp"], tz=timezone.utc).isoformat()
        print(
            f"block {block['number']}  txs {len(block['transactions'])}  "
            f"{block['hash'][:10]}...  {ts}"
        )


if __name__ == "__main__":
    main()
