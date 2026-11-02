
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select amount_out
from "blockchain_db"."public"."int_dex_swap"
where amount_out is null



  
  
      
    ) dbt_internal_test