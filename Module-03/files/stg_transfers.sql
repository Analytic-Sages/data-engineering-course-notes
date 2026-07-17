{{
    config(
        materialized = 'table',
        schema = 'staging',
        alias = 'staged_transfers',
        
    )
}}


with raw_transfers as (
    select * from {{ source('dataset', 'Transfer') }}
)

select
    id as transfer_id,
    to_timestamp("blockTimestamp") as block_time,
    lower("from") as sender_address,
    lower("to") as recipient_address,
    value as raw_value,
    "blockNumber" as block_number,
    txhash,
    lower("tokenAddress") as token_address
from raw_transfers
where value > 0