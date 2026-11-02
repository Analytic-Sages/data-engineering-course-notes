
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select price_token1_per_token0
from "blockchain_db"."public"."int_dex_swap"
where price_token1_per_token0 is null



  
  
      
    ) dbt_internal_test