with codes as (

    select
        *
    
    from {{ source('public','sku_code_translation')}}
)

select * from codes