
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select token_out_symbol
from "blockchain_db"."public"."int_dex_swap"
where token_out_symbol is null



  
  
      
    ) dbt_internal_test