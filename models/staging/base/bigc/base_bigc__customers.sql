{# Creates a base view of BigC customer data from Panoply #}

with source as (
        
        select * from {{ source('public','bigc_tastes_of_chicago_customers') }}
        
)
    
select * from source