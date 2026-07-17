{{
    config(
        materialized = 'table',
        alias='transfers',
        schema='intermediate'
        )
    }}

with staging_transfers as (
    select * from {{ref('stg_transfers')}}
),
meta as (
    select 
        lower(token_address) as token_address,
        symbol,
        decimals,
        token_name
    from {{ref('token_metadata')}}
)


select
    t.txhash,
    block_time,
    t.token_address,
    m.symbol,
    t.sender_address,
    t.recipient_address,
    t.raw_value,
    (t.raw_value / power(10, m.decimals)) as normalized_amount
from staging_transfers t
join meta m on t.token_address = m.token_address
