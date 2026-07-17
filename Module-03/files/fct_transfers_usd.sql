{{
    config(
        materialized='table',
        schema='fact',
        alias='transfers'
    )
}}


with normalized as (
    select * from {{ ref('int_transfers') }}
),
prices as (
    select * from {{ source('dataset', 'token_prices') }} 
)
select
    n.txhash,
    n.block_time,
    n.symbol,
    n.sender_address,
    n.recipient_address,
    n.token_address,
    n.normalized_amount,
    coalesce(p.price_usd, 0) as token_price,
    (n.normalized_amount * coalesce(p.price_usd, 0)) as usd_value
from normalized n
left join prices p 
    on n.token_address = p.token_address 
    and n.block_time = p.price_date