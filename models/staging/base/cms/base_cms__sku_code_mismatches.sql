with mismatches as (

    select
        *
    
    from {{ source('public','sku_code_mismatches')}}
)

select * from mismatches