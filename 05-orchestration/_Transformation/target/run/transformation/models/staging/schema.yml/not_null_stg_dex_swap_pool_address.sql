
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pool_address
from "blockchain_db"."public"."stg_dex_swap"
where pool_address is null



  
  
      
    ) dbt_internal_test