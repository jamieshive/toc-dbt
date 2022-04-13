with items as (

    select
        *
    
    from {{ source('public','tastes_order_items')}}
)

select * from items