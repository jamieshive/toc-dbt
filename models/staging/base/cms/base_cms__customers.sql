{# Creates a base view of customer data from Panoply #}

with source as (
        
    select * from {{ source('public','tastes_customers') }}
        
)
 
select * from source