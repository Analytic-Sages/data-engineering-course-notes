
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select log_index
from "blockchain_db"."public"."int_dex_swap"
where log_index is null



  
  
      
    ) dbt_internal_test