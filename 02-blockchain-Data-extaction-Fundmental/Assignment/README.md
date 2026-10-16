## Assignment

Build an extraction pipeline that pulls the swaps that happened in these three pools and saves them in Postgres.

Pools (Ethereum mainnet):
- 0x2dF380544B88AdB3ad0A94100dcC45fd705aAE2d — XDP / USDC
- 0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640 — ETH / USDC
- 0x0E4831319A50228B9e450861297aB92dee15B44F — WBTC / USDC

The script must accept a block range to extract. Example:

```bash
uv run swap_pipeline.py --from 20000000 --to 20000199
```

What it should do:
1. Fetch Swap logs for each pool in that range. Fetch in small chunks (e.g. 10 blocks per request) so the RPC does not cap you.
2. Decode each log with the pool ABI (Swap event) using `process_log`.
3. Save every decoded swap into Postgres in a `swaps` table with pool_address, block_number, tx_hash, log_index, sender, recipient, amount0, amount1, sqrtPriceX96, liquidity, tick, timestamp. Re-running the same range should not create duplicates.

Submit your script plus a short note showing row counts per pool from Postgres.