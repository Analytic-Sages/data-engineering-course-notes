"""Week 1 lesson build: talk to an Ethereum node over raw JSON-RPC.

Everything is inline and explicit: the RPC URL is a plain constant (no env
loader), the httpx POST carries a hand-built JSON-RPC payload, and hex values
are converted field by field. Run it directly:

   uv run block_explorer.py --latest
   uv run block_explorer.py --block 19000000

Note: RPC_URL below is your personal Infura key. Keep this file out of any
public repository, and rotate the key in the Infura dashboard if it leaks.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Any

import httpx

# Step 1: the node address, used directly. Get your own free key at
# https://app.infura.io (API Keys -> New API Key, network Ethereum).
RPC_URL = "https://mainnet.infura.io/v3/0179c83f73144d9eb1216769ae3f8881"


def rpc_call(url: str, method: str, params: list) -> Any:
    """Step 2: send one JSON-RPC request and return its result."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    response = httpx.post(url, json=payload, timeout=30)
    if response.status_code >= 400:
        raise RuntimeError(f"HTTP {response.status_code} from {method}")
    body = response.json()
    if "error" in body:  # nodes report problems as JSON-RPC errors, not HTTP errors
        raise RuntimeError(
            f"JSON-RPC error {body['error'].get('code')} on {method}: "
            f"{body['error'].get('message')}"
        )
    return body["result"]


def to_iso(hex_timestamp: str | None) -> str | None:
    if hex_timestamp is None:
        return None
    return datetime.fromtimestamp(int(hex_timestamp, 16), tz=timezone.utc).isoformat()


def wei_to_eth(wei_hex: str) -> float:
    return int(wei_hex, 16) / 10**18


def print_block_summary(block: dict) -> None:
    print("=" * 72)
    print(f"BLOCK {int(block['number'], 16)}")
    print("=" * 72)
    print(f"  hash            : {block['hash']}")
    print(f"  parent_hash     : {block['parentHash']}")
    print(f"  timestamp       : {to_iso(block['timestamp'])} (UTC)")
    print(f"  fee_recipient   : {block.get('miner')}")
    print(f"  gas_used        : {int(block['gasUsed'], 16)}")
    print(f"  gas_limit       : {int(block['gasLimit'], 16)}")
    print(f"  base_fee_per_gas: {int(block.get('baseFeePerGas', '0x0'), 16)} wei")
    print(f"  tx_count        : {len(block['transactions'])}")


def print_transaction_summary(tx: dict) -> None:
    to = tx.get("to") or "(contract creation)"
    value_eth = wei_to_eth(tx["value"]) if tx.get("value") else 0.0
    input_len = (len(tx.get("input", "0x")) - 2) // 2
    print(
        f"  tx {tx['hash'][:18]}...  from={tx['from'][:12]}...  "
        f"to={str(to)[:12]}...  value={value_eth:.6f} ETH  "
        f"gas={int(tx['gas'], 16)}  input={input_len} bytes"
    )


def explore(url: str, block_ref: str, limit: int) -> None:
    block = rpc_call(url, "eth_getBlockByNumber", [block_ref, True])
    if block is None:
        raise SystemExit(f"No block found for tag/number: {block_ref}")
    print_block_summary(block)
    print("\ntransactions:")
    for tx in block["transactions"][:limit]:
        print_transaction_summary(tx)
    if len(block["transactions"]) > limit:
        print(f"  ... and {len(block['transactions']) - limit} more")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ethereum block explorer")
    parser.add_argument("--block", help="block number (int) or 'latest'")
    parser.add_argument("--latest", action="store_true", help="shortcut for the latest block")
    parser.add_argument("--limit", type=int, default=5, help="max transactions to print")
    args = parser.parse_args()

    url = RPC_URL

    if args.block:
        block_ref = hex(int(args.block))
    elif args.latest:
        block_ref = "latest"
    else:
        latest = rpc_call(url, "eth_blockNumber", [])  # -> hex like 0x...
        print(f"latest block number: {int(latest, 16)}\n")
        block_ref = "latest"

    explore(url, block_ref, args.limit)


if __name__ == "__main__":
    main()
