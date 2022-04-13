with analytics as (

    select
        *
    
    from {{ source('fivetran_google_analytics','overview')}}
)

select * from analytics