"""Mini analysis over the transactions parquet written by extractor.py.

    uv run analyze.py
    uv run analyze.py --in transactions_19000000_19000010.parquet
"""

import argparse
from pathlib import Path

WEI_PER_ETH = 10**18


def find_default_parquet() -> str:
    """Newest transactions_*.parquet next to this script, so no CLI needed."""
    candidates = sorted(Path(__file__).resolve().parent.glob("transactions_*.parquet"))
    if not candidates:
        raise SystemExit("no transactions_*.parquet found beside analyze.py")
    return str(candidates[-1])


def load(path: str):
    """Read parquet; convert decimal-string wei columns back to ints."""
    import pandas as pd

    df = pd.read_parquet(path)
    for col in ("value_wei", "gas_price_wei"):
        df[col] = df[col].map(lambda x: None if x is None else int(x))
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Mini analysis of tx parquet")
    parser.add_argument(
        "--in", dest="in_path", default=None,
        help="parquet file to read (default: newest transactions_*.parquet here)",
    )
    args = parser.parse_args()

    path = args.in_path or find_default_parquet()
    df = load(path)

    n_blocks = df["block_number"].nunique()
    first, last = int(df["block_number"].min()), int(df["block_number"].max())
    creations = int(df["to_address"].isna().sum())
    zero_value = int((df["value_wei"] == 0).sum())
    total_eth = df["value_wei"].sum() / WEI_PER_ETH

    print(f"file: {Path(path).name}")
    print(f"transactions: {len(df)} across {n_blocks} blocks ({first}-{last})")
    print(f"total value moved: {total_eth:,.4f} ETH")
    print(f"median value per tx: {df['value_wei'].median() / WEI_PER_ETH:.6f} ETH")
    print(f"zero-value txs: {zero_value}")
    print(f"contract creations (no to_address): {creations}")
    print(f"avg gas per tx: {df['gas'].mean():,.0f}")
    print(f"avg gas price: {df['gas_price_wei'].mean() / 1e9:,.2f} gwei")
    print("txs per block:")
    print(df.groupby("block_number").size().to_string())
    print("top 5 senders by tx count:")
    print(df["from_address"].value_counts().head(5).to_string())
    

if __name__ == "__main__":
    main()    