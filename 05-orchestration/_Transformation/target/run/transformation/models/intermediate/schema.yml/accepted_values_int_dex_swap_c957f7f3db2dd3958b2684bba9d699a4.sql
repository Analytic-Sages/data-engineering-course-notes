
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

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



  
  
      
    ) dbt_internal_test