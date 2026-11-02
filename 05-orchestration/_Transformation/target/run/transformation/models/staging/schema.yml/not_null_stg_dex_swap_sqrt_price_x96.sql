
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sqrt_price_x96
from "blockchain_db"."public"."stg_dex_swap"
where sqrt_price_x96 is null



  
  
      
    ) dbt_internal_test