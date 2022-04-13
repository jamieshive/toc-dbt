with new_forecast as (

    select
        *
    
    from {{ source('public','april_reforecast')}}
)

select * from new_forecast