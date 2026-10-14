"""Module 03 — Standalone ERC-20 Transfer Event Indexer.

This indexer queries Ethereum RPC nodes for `Transfer(from, to, value)` event logs
emitted across smart contracts within a block window (or filtered to a specific token address).

Self-contained script: Saves decoded transfer transactions directly to CSV or Parquet format.

Run from `docs/03-web3-rpc/src`:
    python erc20_transfer_indexer.py --from 20000000 --to 20000002
    python erc20_transfer_indexer.py --blocks 10 --token usdc --out transfers.csv
    python erc20_transfer_indexer.py --blocks 10 --token usdc --out transfers.parquet
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time

from web3 import Web3
from web3.exceptions import LogTopicError, MismatchedABI

# Import local contract constants directly
from contracts import ERC20_MINIMAL_ABI, TRANSFER_TOPIC0, USDC_ADDRESS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("erc20_transfer_indexer")

PUBLIC_RPC_ENDPOINTS = [
    os.getenv("WEB3_RPC_URL"),
    "https://eth.drpc.org",
    "https://1rpc.io/eth",
    "https://ethereum-rpc.publicnode.com",
    "https://eth.merkle.io",
]
PUBLIC_RPC_ENDPOINTS = [url for url in PUBLIC_RPC_ENDPOINTS if url]


# ==============================================================================
# SECTION 1: Helpers (Address & File Output)
# ==============================================================================

def checksum(address: str) -> str:
    """Convert an address string to EIP-55 checksummed format."""
    return Web3.to_checksum_address(address)


TRANSFER_FIELDNAMES = [
    "block_number",
    "tx_hash",
    "log_index",
    "token_address",
    "from_address",
    "to_address",
    "value_wei",
    "timestamp",
]


def save_transfers_to_csv(transfers: list[dict], filepath: str) -> None:
    """Save decoded token transfers directly to a CSV file."""
    dir_name = os.path.dirname(os.path.abspath(filepath))
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)

    fieldnames = TRANSFER_FIELDNAMES

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(transfers)

    print(f"\n💾 Saved {len(transfers)} decoded transfer transactions to '{filepath}'")


def save_transfers_to_parquet(transfers: list[dict], filepath: str) -> None:
    """Save decoded token transfers directly to a Parquet file.

    Requires ``pandas`` with the ``pyarrow`` engine
    (``pip install pandas pyarrow``).

    Note: ``value_wei`` is a uint256 and can exceed int64 max, so it is
    stored as a string in Parquet to avoid ``OverflowError``.
    Cast it back with ``int(value_wei)`` when reading.
    """
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError(
            "Saving to Parquet requires pandas and pyarrow. "
            "Install them with: pip install pandas pyarrow"
        ) from exc

    dir_name = os.path.dirname(os.path.abspath(filepath))
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)

    # Preserve column order (and schema for the empty case).
    df = pd.DataFrame(transfers, columns=TRANSFER_FIELDNAMES)
    # uint256 (up to 2**256-1) does not fit in int64 — store as string.
    if "value_wei" in df.columns:
        df["value_wei"] = df["value_wei"].astype(str)
    try:
        df.to_parquet(filepath, engine="pyarrow", index=False)
    except ImportError as exc:
        raise ImportError(
            "Saving to Parquet requires the pyarrow engine. "
            "Install it with: pip install pyarrow"
        ) from exc

    print(f"\n💾 Saved {len(transfers)} decoded transfer transactions to '{filepath}'")


def save_output(transfers: list[dict], filepath: str) -> None:
    """Save decoded transfer records to CSV, Parquet, JSON, or JSONL based on file extension."""
    lowered = filepath.lower()
    if lowered.endswith(".parquet") or lowered.endswith(".pq"):
        save_transfers_to_parquet(transfers, filepath)
    elif filepath.endswith(".json"):
        dir_name = os.path.dirname(os.path.abspath(filepath))
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(transfers, f, indent=2)
        print(f"\n💾 Saved {len(transfers)} transfer transactions to '{filepath}'")
    elif filepath.endswith(".jsonl"):
        dir_name = os.path.dirname(os.path.abspath(filepath))
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            for t in transfers:
                f.write(json.dumps(t) + "\n")
        print(f"\n💾 Saved {len(transfers)} transfer transactions to '{filepath}'")
    else:
        save_transfers_to_csv(transfers, filepath)


# ==============================================================================
# SECTION 2: Main Indexer Class
# ==============================================================================

class ERC20TransferIndexer:
    def __init__(self, token_address: str | None = None, rpc_url: str | None = None):
        """Initialize indexer with Web3 HTTP provider."""
        self.token_address = token_address.lower() if token_address else None
        self.rpc_endpoints = [rpc_url] if rpc_url else PUBLIC_RPC_ENDPOINTS
        self.current_rpc_idx = 0
        self.w3 = self._init_web3(self.rpc_endpoints[0])
        
        dummy_address = checksum(self.token_address or USDC_ADDRESS)
        self.contract = self.w3.eth.contract(address=dummy_address, abi=ERC20_MINIMAL_ABI)

    def _init_web3(self, url: str) -> Web3:
        return Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))

    def rpc_call(self, fn_name: str, *args, **kwargs):
        """Execute Web3 call with automatic fallback across public RPC endpoints."""
        last_exception = None
        for _ in range(len(self.rpc_endpoints) * 2):
            url = self.rpc_endpoints[self.current_rpc_idx]
            try:
                if fn_name == "get_block_number":
                    return self.w3.eth.get_block_number()
                elif fn_name == "get_logs":
                    return self.w3.eth.get_logs(*args, **kwargs)
                elif fn_name == "get_block":
                    return self.w3.eth.get_block(*args, **kwargs)
            except Exception as err:
                last_exception = err
                logger.warning("RPC %s error: %s. Trying next endpoint...", url, err)
                self.current_rpc_idx = (self.current_rpc_idx + 1) % len(self.rpc_endpoints)
                self.w3 = self._init_web3(self.rpc_endpoints[self.current_rpc_idx])
                dummy_address = checksum(self.token_address or USDC_ADDRESS)
                self.contract = self.w3.eth.contract(address=dummy_address, abi=ERC20_MINIMAL_ABI)
                time.sleep(1.0)
        raise last_exception

    def latest_block(self) -> int:
        """Fetch current latest block number from RPC node."""
        return self.rpc_call("get_block_number")

    def fetch_transfer_logs(self, from_block: int, to_block: int) -> list[dict]:
        """Fetch raw Transfer logs across all tokens (or specific token) in block range."""
        target = self.token_address or "ALL ERC-20 Tokens"
        logger.info("eth_getLogs %s..%s for %s", from_block, to_block, target)
        
        filter_params = {
            "fromBlock": from_block,
            "toBlock": to_block,
            "topics": [TRANSFER_TOPIC0],
        }
        if self.token_address:
            filter_params["address"] = checksum(self.token_address)

        logs = self.rpc_call("get_logs", filter_params=filter_params)
        logger.info("  -> %d Transfer logs found", len(logs))
        return logs

    def decode_transfer(self, log: dict) -> dict | None:
        """Decode an EVM Transfer log using Web3.py ABI event processing."""
        if len(log.get("topics", [])) != 3:
            return None
        try:
            event = self.contract.events.Transfer()
            decoded = event.process_log(log)
            return decoded["args"]
        except (LogTopicError, MismatchedABI, ValueError):
            return None

    def _get_block_timestamp(self, block_number: int) -> int:
        """Fetch the timestamp of a given block."""
        try:
            block = self.rpc_call("get_block", block_number)
            return block["timestamp"]
        except Exception as err:
            logger.warning("Failed to get timestamp for block %d: %s", block_number, err)
            return 0

    def _get_block_timestamps(self, block_numbers: set[int]) -> dict[int, int]:
        """Fetch timestamps for a set of block numbers (batched to minimize RPC calls)."""
        timestamps: dict[int, int] = {}
        for bn in sorted(block_numbers):
            if bn not in timestamps:
                timestamps[bn] = self._get_block_timestamp(bn)
        return timestamps

    def index_window(self, from_block: int, to_block: int) -> list[dict]:
        """Process logs in a block window and return clean decoded transfer transactions."""
        transfers: list[dict] = []
        logs = self.fetch_transfer_logs(from_block, to_block)

        # Collect unique block numbers to fetch timestamps in one pass
        block_numbers = {log["blockNumber"] for log in logs}
        block_timestamps = self._get_block_timestamps(block_numbers)

        for log in logs:
            args = self.decode_transfer(log)
            if not args:
                continue

            block_number = log["blockNumber"]
            tx_hash = log["transactionHash"].hex() if isinstance(log["transactionHash"], bytes) else log["transactionHash"]
            if isinstance(tx_hash, str) and not tx_hash.startswith("0x"):
                tx_hash = "0x" + tx_hash
            log_index = log["logIndex"]
            
            contract_address = log["address"].lower() if isinstance(log["address"], str) else log["address"]

            from_addr, to_addr = args["from"].lower(), args["to"].lower()
            timestamp = block_timestamps.get(block_number, 0)

            # Clean decoded transfer transaction row
            transfers.append({
                "block_number": block_number,
                "tx_hash": tx_hash,
                "log_index": log_index,
                "token_address": contract_address,
                "from_address": from_addr,
                "to_address": to_addr,
                "value_wei": args["value"],
                "timestamp": timestamp,
            })

        return transfers

    def print_transfers(self, transfers: list[dict]) -> None:
        """Pretty-print decoded transfers to stdout."""
        print(f"\n{len(transfers)} ERC-20 Transfer transactions decoded:")
        print(f"{'block':>9}  {'token contract':<14} {'from':<12} {'to':<12} {'raw value':>18}")
        for t in transfers[:25]:
            print(
                f"{t['block_number']:>9}  {t['token_address'][:12]:<14} "
                f"{t['from_address'][:10]:<12} {t['to_address'][:10]:<12} {t['value_wei']:>18,}"
            )
        if len(transfers) > 25:
            print(f"... and {len(transfers) - 25} more")


def main() -> None:
    parser = argparse.ArgumentParser(description="Standalone ERC-20 Transfer Event Indexer")
    parser.add_argument("--blocks", type=int, default=5,
                        help="window size in blocks ending at latest block")
    parser.add_argument("--from", "--from-block", type=int, dest="from_block",
                        help="starting block number")
    parser.add_argument("--to", "--to-block", type=int, dest="to_block",
                        help="ending block number")
    parser.add_argument("--address", help="specific ERC-20 contract address (default: all tokens)")
    parser.add_argument("--token", help="token shortcut (e.g. usdc)")
    parser.add_argument("--rpc-url", help="custom Web3 RPC endpoint URL")
    parser.add_argument("--out", default="transfers.csv",
                        help="output file path (format inferred from extension: .csv, .parquet/.pq, .json, .jsonl; default: transfers.csv)")
    parser.add_argument("--format", choices=["csv", "parquet", "json", "jsonl"], default=None,
                        help="explicit output format (overrides --out extension; e.g. --format parquet)")
    args = parser.parse_args()

    token_address = args.address
    if not token_address and args.token and args.token.lower() == "usdc":
        token_address = USDC_ADDRESS

    indexer = ERC20TransferIndexer(token_address=token_address, rpc_url=args.rpc_url)
    latest = indexer.latest_block()
    
    if args.from_block is not None:
        from_block = args.from_block
        to_block = args.to_block if args.to_block is not None else from_block
    else:
        to_block = latest
        from_block = max(0, latest - args.blocks + 1)

    transfers = indexer.index_window(from_block, to_block)
    indexer.print_transfers(transfers)

    # Save clean transfer rows (format from --format flag or --out extension)
    out_path = args.out or "transfers.csv"
    if args.format == "parquet":
        if not out_path.lower().endswith((".parquet", ".pq")):
            out_path = os.path.splitext(out_path)[0] + ".parquet"
        save_transfers_to_parquet(transfers, out_path)
    elif args.format == "csv":
        if not out_path.lower().endswith(".csv"):
            out_path = os.path.splitext(out_path)[0] + ".csv"
        save_transfers_to_csv(transfers, out_path)
    elif args.format in ("json", "jsonl"):
        base, _ = os.path.splitext(out_path)
        out_path = base + (".json" if args.format == "json" else ".jsonl")
        save_output(transfers, out_path)
    else:
        save_output(transfers, out_path)


if __name__ == "__main__":
    main()
