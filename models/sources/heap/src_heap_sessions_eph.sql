
WITH 
    sessions_base 
        AS 
            (
                SELECT 
                    s.session_id AS heap_session_id 
                  , s.user_id AS heap_user_id 
                  , s.time AS heap_session_start_time
                  , s.library AS heap_session_source 
                  , s.platform AS heap_session_platform 
                  , s.device_type AS heap_session_device_type
                  , s.device AS heap_session_device_model 
                  , s.country AS heap_session_country 
                  , s.region AS heap_session_region 
                  , s.city AS heap_session_city 
                  , s.ip AS heap_session_ip 
                  , s.referrer AS heap_session_referrer 
                  , s.landing_page AS heap_session_landing_page
                  , s.landing_page_query AS heap_session_landing_page_query 
                  , s.landing_page_hash AS heap_session_landing_page_hash 
                  , s.browser AS heap_session_browser 
                  , s.utm_source AS heap_session_utm_source 
                  , s.utm_campaign AS heap_session_utm_campaign 
                  , s.utm_medium AS heap_session_utm_medium 
                  , s.utm_term AS heap_session_utm_term 
                  , s.utm_content AS heap_session_utm_content 
                  
                FROM {{source('heap','sessions')}} s 
                {% if is_incremental() %}
                    -- abstracted the incremental logic away from the incrementally materialized tables found within the staging directory
                    -- creates a handoff point from an architect or data eng to analysts that are tasked with building dashboards or running analysis 
                    WHERE s.user_id IN ( {{get_active_users('heap_session_start_time')}} )
                {% endif %}
            ), 
    sessions_deduped
        AS 
            (
                -- https://github.com/dbt-labs/dbt-utils#deduplicate-source
                {{ dbt_utils.deduplicate(
                    relation='sessions_base',
                    partition_by='heap_session_id',
                    order_by="heap_session_start_time desc",
                )
                }}
            )

SELECT
      s.*
      -- generate the order in which each session occurred for each user
    , ROW_NUMBER() OVER(PARTITION BY s.heap_user_id ORDER BY s.heap_session_start_time) AS heap_session_sequence_num
FROM sessions_deduped s