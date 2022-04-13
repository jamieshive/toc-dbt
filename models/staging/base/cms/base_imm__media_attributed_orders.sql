{# Creates a base view of IMM data from Panoply #}

with source as (
        
        select * from {{ source('public', 'imm_media_attributed_orders') }}
        
)
    
select * from source