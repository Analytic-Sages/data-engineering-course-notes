
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select tick
from "blockchain_db"."public"."stg_dex_swap"
where tick is null



  
  
      
    ) dbt_internal_test