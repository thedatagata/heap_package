WITH 
    pageviews_base 
        AS 
            (
                SELECT * 
                FROM {{source('heap','pageviews')}} s 
                {% if is_incremental() %}
                    WHERE s.heap_user_id IN ( {{get_active_users('heap_pageview_time')}} )
                {% endif %}
            )

{{ dbt_utils.deduplicate(
    relation='pageviews_base',
    partition_by='heap_user_id, heap_pageview_id',
    order_by="heap_pageview_time desc",
)
}}