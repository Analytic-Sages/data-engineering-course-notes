"""Reference Smart Contract Configuration for USDC on Ethereum.

This file defines the canonical reference token (USDC) and its minimal ERC-20 ABI.
It exports simple constants so extraction code is clean and readable.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# ==============================================================================
# USDC Token Constants
# ==============================================================================

_USDC_MAINNET_DEFAULT = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"

USDC_ADDRESS = os.getenv("USDC_MAINNET_ADDRESS", _USDC_MAINNET_DEFAULT).lower()
USDC_SYMBOL = "USDC"
USDC_NAME = "USD Coin"
USDC_DECIMALS = 6

# Canonical topic0 for ERC-20 Transfer — derived from keccak256("Transfer(address,address,uint256)")
TRANSFER_TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Minimal ERC-20 ABI (Transfer & Approval events + view methods)
ERC20_MINIMAL_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "name",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [{"name": "owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "from", "type": "address"},
            {"indexed": True, "name": "to", "type": "address"},
            {"indexed": False, "name": "value", "type": "uint256"},
        ],
        "name": "Transfer",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "owner", "type": "address"},
            {"indexed": True, "name": "spender", "type": "address"},
            {"indexed": False, "name": "value", "type": "uint256"},
        ],
        "name": "Approval",
        "type": "event",
    },
]


@dataclass(frozen=True)
class TokenConfig:
    symbol: str = USDC_SYMBOL
    chain: str = "mainnet"
    address: str = USDC_ADDRESS
    name: str = USDC_NAME
    decimals: int = USDC_DECIMALS
    abi: list = None

    def __post_init__(self):
        if self.abi is None:
            object.__setattr__(self, "abi", ERC20_MINIMAL_ABI)


def get_token(symbol: str = "usdc", chain: str | None = None) -> TokenConfig:
    """Return the TokenConfig for the requested token."""
    return TokenConfig()
