
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select amount0_raw
from "blockchain_db"."public"."stg_dex_swap"
where amount0_raw is null



  
  
      
    ) dbt_internal_test