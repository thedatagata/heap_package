
{{
    config(
        unique_key='heap_session_id'
    )
}}

-- here is the dbt util function for pivot (https://github.com/dbt-labs/dbt-utils#pivot-source)
WITH 
  funnel_pivot 
    AS 
        (
          SELECT
              e.heap_user_id 
            , e.heap_session_id 
            , {{ dbt_utils.pivot('heap_event_funnel_position', dbt_utils.get_column_values(ref('stg_heap_events_fct'),'heap_event_funnel_position'), prefix='funnel_position_'}}
            , SUM(CASE WHEN e.heap_is_conversion_event THEN 1 ELSE 0 END) as heap_session_conversion_cnt
          -- we only want to update users who have had activity since the last run 
          FROM {{ ref('stg_heap_events_all_inc_fct') }} e 
          WHERE e.heap_event_funnel_position IS NOT NULL 
          {% if is_incremental() %}
            AND e.heap_user_id IN ({{get_active_users('heap_session_start_time')}})
          {% endif %}
          GROUP BY 1,2
        ), 

SELECT
    s.heap_session_id
  , s.heap_session_sequence_num 
  , s.heap_session_start_time 
  , TIMESTAMP_ADD(TIMESTAMP s.heap_session_start_time, INTERVAL -14 DAY) AS heap_session_conversion_lookback_14d
  , TIMESTAMP_ADD(TIMESTAMP s.heap_session_start_time, INTERVAL 14 DAY) AS heap_session_conversion_lookforward_14d
  , s.heap_user_id 
  , s.heap_session_country
  , s.heap_session_region
  , s.heap_session_city 
  , s.heap_session_ip 
  , s.heap_session_referrer 
  , s.heap_session_landing_page
  , s.heap_session_landing_page_query 
  , s.heap_session_landing_page_hash 
  , s.heap_session_browser
  , s.heap_session_utm_source
  , s.heap_session_utm_campaign
  , s.heap_session_utm_medium 
  , s.heap_session_utm_term 
  , s.heap_session_utm_content
  , s.heap_defined_channel_attribution
  , fp.*
  , CASE WHEN fp.heap_session_conversion_cnt > 0 THEN 1 ELSE 0 END AS heap_session_is_conversion


FROM {{ ref('src_heap_sessions_eph') }} s
LEFT JOIN funnel_pivot fp
    ON s.heap_user_id = fp.heap_user_id
    AND s.heap_session_id = fp.heap_session_id 