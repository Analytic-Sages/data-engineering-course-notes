
    
    

with all_values as (

    select
        direction as value_field,
        count(*) as n_records

    from "blockchain_db"."public"."int_dex_swap"
    group by direction

)

select *
from all_values
where value_field not in (
    'token0_to_token1','token1_to_token0'
)


