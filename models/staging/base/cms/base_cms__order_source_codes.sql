{# Creates a base view of order source code data from Panoply #}

with source as (
        
        select * from {{ source('public', 'order_src_codes') }} 
        
)
    
select * from source