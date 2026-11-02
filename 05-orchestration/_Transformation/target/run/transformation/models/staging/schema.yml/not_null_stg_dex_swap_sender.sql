
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sender
from "blockchain_db"."public"."stg_dex_swap"
where sender is null



  
  
      
    ) dbt_internal_test