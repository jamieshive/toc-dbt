with ads as (

    select
        *

    from {{ ref('google_ads__url_ad_adapter')}}
)

select * from ads