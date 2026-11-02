
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select tx_hash
from "blockchain_db"."public"."int_dex_swap"
where tx_hash is null



  
  
      
    ) dbt_internal_test