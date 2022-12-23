WITH 
    pageviews_base 
        AS 
            (
                SELECT 
                    p.event_id AS heap_pageview_id 
                  , p.time AS heap_pageview_time 
                  , p.user_id AS heap_user_id 
                  , p.session_id AS heap_session_id 
                  , p.domain AS heap_pageview_domain
                  , p.path AS heap_pageview_path 
                  , p.query AS heap_pageview_query 
                  , p.hash AS heap_pageview_hash 
                  , p.title AS heap_pageview_title 
                  , p.heap_previous_page AS heap_pageview_previous_page
                  , p.gtm_user_data_visitor_type
                  , p.gtm_user_data_total_order_count 
                  , p.gtm_user_data_cltv 
                  , p.gtm_user_data_is_new_user
                  , p.gtm_cart_data_cart_value
                  
                FROM {{source('heap','pageviews')}} p
                {% if is_incremental() %}
                    -- abstracted the incremental logic away from the incrementally materialized tables found within the staging directory
                    -- creates a handoff point from an architect or data eng to analysts that are tasked with building dashboards or running analysis 
                    WHERE s.user_id IN ( {{get_active_users('heap_pageview_time')}} )
                {% endif %}
            )
-- https://github.com/dbt-labs/dbt-utils#deduplicate-source
{{ dbt_utils.deduplicate(
    relation='pageviews_base',
    partition_by='heap_pageview_id',
    order_by="heap_pageview_time desc",
)
}}