{{
    config(
        materialized='table'
    )
}}

select * from {{ ref('int_dex_swap') }}