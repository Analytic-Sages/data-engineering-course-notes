# Module 6 (extract) — Uniswap V3 Swap Extractor

Extracts `Swap` events from a Uniswap V3 pool over **any block range** via
JSON-RPC (`eth_getLogs` + web3.py), **decodes** the raw log data into typed
fields, and loads them into **PostgreSQL** (Docker).

Default pool: `0x5777d92f208679db4b9778590fa3cab3ac9e2168` — the Uniswap V3
**DAI/USDC 0.05%** pool on Ethereum mainnet. `--pool` accepts any Uniswap V3
pool; token0/token1 symbols and decimals are read from the chain automatically.

## Quickstart

```bash
cd docs/06-dbt-analytics/extract
cp .env.example .env          # add your WEB3_RPC_URL
docker compose up -d          # PostgreSQL on localhost:5432 (blockchain_db)
uv sync                       # web3 + psycopg + dotenv

# Extract blocks 19,000,000–19,000,100 and load into Postgres
uv run extract_swaps.py --from-block 19000000 --to-block 19000100

# Any start block; --to-block defaults to the chain head
uv run extract_swaps.py --from-block 19200000

# Decode only, inspect a sample, no DB write
uv run extract_swaps.py --from-block 19000000 --to-block 19000100 --dry-run --csv swaps.csv
```

## CLI

| Flag | Required | Default | Meaning |
|---|---|---|---|
| `--from-block` | yes | — | first block (inclusive) |
| `--to-block` | no | chain head | last block (inclusive) |
| `--pool` | no | DAI/USDC 0.05% | any Uniswap V3 pool address |
| `--chunk-size` | no | 2000 | blocks per `eth_getLogs` call |
| `--db-url` | no | built from `DB_*` env | full PostgreSQL URL |
| `--csv PATH` | no | — | also dump decoded rows to CSV |
| `--dry-run` | no | — | extract + decode, skip DB load |

## What "decode" means here

Each raw log is:

- **topics[0]** matched against the canonical `Swap` event signature hash;
- **sender/recipient** recovered from the indexed topics;
- **amount0/amount1** (`int256`) decoded from `data` — negative = pool paid user;
- amounts scaled to human units using each token's on-chain `decimals()`;
- **sqrtPriceX96/liquidity/tick** decoded from the remaining ABI words.

## Table: `public.pool_swaps`

Natural key `(tx_hash, log_index)` with `ON CONFLICT DO NOTHING` — re-running
any block range is **idempotent** (duplicates are skipped, not doubled).

| Column | Type | Notes |
|---|---|---|
| pool_address | text | lowercase pool address |
| block_number / block_timestamp | bigint / timestamptz | where the swap happened |
| tx_hash / tx_index / log_index | text / int / int | log coordinates |
| sender / recipient | text | indexed Swap params |
| amount0_raw / amount1_raw | numeric(78,0) | wei-scale, signed |
| amount0 / amount1 | numeric(78,18) | decimals-adjusted, signed |
| sqrt_price_x96 / liquidity / tick | numeric / numeric / int | pool state at swap |
| raw_data | text | original log data hex (audit trail) |

```sql
select * from pool_swaps order by block_number desc limit 10;
```
