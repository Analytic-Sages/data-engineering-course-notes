

with swaps as (

    select * from {{ ref('stg_dex_swap') }}

),

-- sqrt(price) in raw units: undo the x96 fixed-point scale with power(2, 96).
-- The ::numeric cast keeps the division exact (an untyped power(2, 96) is
-- double precision and would drag the whole ratio into float).
priced as (

    select
        swaps.*,
        sqrt_price_x96 / power(2::numeric, 96) as sqrt_price_ratio
    from swaps

)

select
    -- keys & context (pass-through)
    tx_hash,
    log_index,
    block_number,
    tx_index,
    pool_address,
    sender,
    recipient,
    block_timestamp

    -- pool state at execution time (pass-through, useful context for marts)
    liquidity,
    tick,

    -- 1. direction: which way did the trade go? (from the pool's signs)
    case
        when amount0_raw > 0 then 'token0_to_token1'
        else 'token1_to_token0'
    end                                                 as direction,

    -- trader perspective: what was sold TO the pool, what was BOUGHT from it
    case when amount0_raw > 0 then 'DAI' else 'USDC' end as token_in_symbol,
    case when amount0_raw > 0 then 'USDC' else 'DAI' end as token_out_symbol,
    case when amount0_raw > 0 then amount0 else amount1 end   as amount_in,
    case when amount0_raw > 0 then -amount1 else -amount0 end as amount_out,

    -- 2. price: token1 per token0 (USDC per DAI for the default pool) ≈ 1.0
    sqrt_price_ratio
        * sqrt_price_ratio
        * power(10::numeric, 12)                       as price_token1_per_token0

from priced
