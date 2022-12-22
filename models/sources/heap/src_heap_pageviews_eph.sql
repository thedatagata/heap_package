WITH 
    pageviews_base 
        AS 
            -- abstracted the incremental logic away from the incrementally materialized tables found within the staging directory
            -- creates a handoff point from an architect or data eng to analysts that are tasked with building dashboards or running analysis 
            (
                SELECT * 
                FROM {{source('heap','pageviews')}} s 
                {% if is_incremental() %}
                    WHERE s.heap_user_id IN ( {{get_active_users('heap_pageview_time')}} )
                {% endif %}
            )
-- https://github.com/dbt-labs/dbt-utils#deduplicate-source
{{ dbt_utils.deduplicate(
    relation='pageviews_base',
    partition_by='heap_user_id, heap_pageview_id',
    order_by="heap_pageview_time desc",
)
}}