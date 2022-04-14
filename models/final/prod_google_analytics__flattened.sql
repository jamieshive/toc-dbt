with final as (

    select
        *

    from {{ ref('int_google_analytics_joined')}}
)

select * from final