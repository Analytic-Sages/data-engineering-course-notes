
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select block_timestamp
from "blockchain_db"."public"."stg_dex_swap"
where block_timestamp is null



  
  
      
    ) dbt_internal_test