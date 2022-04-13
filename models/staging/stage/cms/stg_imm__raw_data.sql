with base as (

    select
        *

    from {{ ref('base_imm__raw_data')}}
),

staged as (

    select
        id,
        geo,
        case 
            when
                transactions = 0
            then
                .00001
            else
                transactions
        end as transactions,
        site1,
        funnel2,
        case 
            when
                clicks = 0
            then
                .00001
            else
                clicks
        end as clicks,
        case 
            when
                "total net spend" = 0
            then
                .00001
            else
                "total net spend"
        end as total_net_spend,
        campaign,
        "audience1.1",
        case 
            when
                "new customers" = 0
            then
                .00001
            else
                "new customers"
        end as new_customers,
        case 
            when
                "estimated corporate orders (50% close rate)" = 0
            then
                .00001
            else
                "estimated corporate orders (50% close rate)"
        end as estimated_corporate_orders,
        customergrouping as customer_grouping,
        case 
            when
                "corp formdownload" = 0
            then
                .00001
            else
                "corp formdownload"
        end as corp_form_download,
        acqretcat,
        platform,
        targetingtype,
        case 
            when
                "new users" = 0
            then
                .00001
            else
                "new users"
        end as new_users,
        funnel,
        case 
            when
                "items sold" = 0
            then
                .00001
            else
                "items sold"
        end as items_sold,
        campaign2,
        toc,
        case 
            when
                "corp contact form" = 0
            then
                .00001
            else
                "corp contact form"
        end as corp_contact_form,
        crtvoffer,
        site,
        case 
            when
                "corporate order inquiry" = 0
            then
                .00001
            else
                "corporate order inquiry"
        end as corporate_order_inquiry,
        case 
            when
                "adserve cost" = 0
            then
                .00001
            else
                "adserve cost"
        end as adserve_cost,
        audience1,
        case 
            when
                "sales revenue" = 0
            then
                .00001
            else
                "sales revenue"
        end as sales_revenue,
        "date"::date as "date",
        case 
            when
                "corp email/phone" = 0
            then
                .00001
            else
                "corp email/phone"
        end as corp_email_phone,
        case 
            when
                "incremental units sold" = 0
            then
                .00001
            else
                "incremental units sold"
        end as incremental_units_sold,
        targetingtype2,
        targetingtype1,
        cta_cat,
        case 
            when
                "adserver impressions" = 0
            then
                .00001
            else
                "adserver impressions"
        end as adserver_impressions,
        case 
            when
                "incremental conversions" = 0
            then
                .00001
            else
                "incremental conversions"
        end as incremental_conversions,
        case 
            when
                "incremental revenue" = 0
            then
                .00001
            else
                "incremental revenue"
        end as incremental_revenue,
        case 
            when
                spend = 0
            then
                .00001
            else
                spend
        end as spend,
        case 
            when
                impressions = 0
            then
                .00001
            else
                impressions
        end as impressions,
        case 
            when
                "estimated corporate revenue" = 0
            then
                .00001
            else
                "estimated corporate revenue"
        end as estimated_corporate_revenue,
        case 
            when
                users = 0
            then
                .00001
            else
                users
        end as users,
        audience2,
        case 
            when
                "returning orders" = 0
            then
                .00001
            else
                "returning orders"
        end as returning_orders,
        case 
            when
                sessions = 0
            then
                .00001
            else
                sessions
        end as sessions,
        case 
            when
                "adserver clicks" = 0
            then
                .00001
            else
                "adserver clicks"
        end as adserver_clicks,
        costtype,
        channel,
        placement,
        case 
            when
                iroas = 0
            then
                .00001
            else
                iroas
        end as iroas,
        case 
            when
                "roas with estimated corp" = 0
            then
                .00001
            else
                "roas with estimated corp"
        end as roas_with_estimated_corp,
        case 
            when
                roas = 0
            then
                .00001
            else
                roas
        end as roas,
        case 
            when
               cpc = 0
            then
                .00001
            else
                cpc
        end as cpc,
        case 
            when
                "avg session duration" = 0
            then
                '.00001'
            else
                "avg session duration"
        end as avg_session_duration,
        case 
            when
                "pages / session" = 0
            then
                .00001
            else
                "pages / session"
        end as pages_per_session,
        case 
            when
                "bounce rate" = 0
            then
                .00001
            else
                "bounce rate"
        end as bounce_rate,
        case 
            when
                "new user rate" = 0
            then
                .00001
            else
                "new user rate"
        end as new_user_rate,
        case 
            when
                icpa = 0
            then
                .00001
            else
                icpa
        end as icpa,
        case 
            when
                cpa = 0
            then
                .00001
            else
                cpa
        end as cpa,
        case 
            when
               aov = 0
            then
                .00001
            else
                aov
        end as aov,
        case 
            when
                "% returned" = 0
            then
                .00001
            else
                "% returned"
        end as percent_returned,
        case 
            when
                "% new customers" = 0
            then
                .00001
            else
                "% new customers"
        end as percent_new_customers,
        case 
            when
                "clicks per session" = 0
            then
                .00001
            else
                "clicks per session"
        end as clicks_per_session,
        case 
            when
                "net new" = 0
            then
                .00001
            else
                "net new"
        end as net_new

    from base
)

select * from staged