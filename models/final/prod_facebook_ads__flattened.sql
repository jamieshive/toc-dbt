with ads as (

    select
        *

    from {{ ref('facebook_ads__ad_adapter')}}
)

select * from ads