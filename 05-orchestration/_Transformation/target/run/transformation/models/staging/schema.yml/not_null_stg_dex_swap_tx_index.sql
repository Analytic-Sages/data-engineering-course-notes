
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select tx_index
from "blockchain_db"."public"."stg_dex_swap"
where tx_index is null



  
  
      
    ) dbt_internal_test