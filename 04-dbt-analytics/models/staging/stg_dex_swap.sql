
with source as (

    select * from {{ source('blockchain', 'pool_swaps') }}

),

renamed as (

    select
        -- natural key (surrogate id stays in the raw layer only)
        tx_hash,
        log_index::integer              as log_index,

        -- identifiers
        pool_address,
        block_number::bigint            as block_number,
        tx_index::integer               as tx_index,
        sender,
        recipient,

        -- decoded swap values — already typed by the extractor, cast only
        amount0_raw::numeric(78, 0)     as amount0_raw,
        amount1_raw::numeric(78, 0)     as amount1_raw,
        amount0::numeric(78, 18)        as amount0,
        amount1::numeric(78, 18)        as amount1,

        -- pool state captured at swap time
        sqrt_price_x96::numeric(78, 0)  as sqrt_price_x96,
        liquidity::numeric(78, 0)       as liquidity,
        tick::integer                   as tick,

        -- timestamp → date grain (UTC)
        block_timestamp
    from source

)
select * from renamed
