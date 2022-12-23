
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
              e.heap_user_id AS funnel_user_id
            , e.heap_session_id AS funnel_session_id 
            -- dbt.pivot('column_name', dbt_utils.get_column_values(ref('model'), 'column_name'), prefix='str to be set in front of each unique value returned from get_column_values as a column name', agg='default is sum')
            -- this returns a count of how many times a user performed a funnel event within each session as defined in the funnel_events variable found within the dbt_project yml file 
            , {{ dbt_utils.pivot('heap_event_funnel_position', dbt_utils.get_column_values(ref('stg_heap_events_all_fct'),'heap_event_funnel_position'), prefix='funnel_position_') }}
            -- this counts the total occurences of a conversion event in a users session 
            , SUM(CASE WHEN e.heap_event_is_conversion THEN 1 ELSE 0 END) as heap_session_conversion_cnt
          -- we only want to update users who have had activity since the last run 
          FROM {{ ref('stg_heap_events_all_fct') }} e 
          WHERE e.heap_event_funnel_position IS NOT NULL
            AND e.heap_event_funnel_position > 0 
          {% if is_incremental() %}
            AND e.heap_user_id IN ({{get_active_users('heap_session_start_time')}})
          {% endif %}
          GROUP BY 1,2
        ) 

SELECT
    s.heap_session_id
  , s.heap_user_id
  , s.heap_session_sequence_num 
  , s.heap_session_start_time
  -- subtract 14 days from the session start time
  -- this enables consumers of this table to look for conversion sessions (see below) and then lookback by 14 days in order to find each touch that lead up to a conversion
  , TIMESTAMPADD(day, -14, s.heap_session_start_time) AS heap_session_conversion_lookback_14d
  -- add 14 days to each session start time
  -- opposite to the above, this allows a consumer of this table to look for paid ads and then look forward to see if any conversions happened within 14 days 
  , TIMESTAMPADD(day, 14, s.heap_session_start_time) AS heap_session_conversion_lookforward_14d
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
  -- used alias star in order to be flexible to varying funnel_event list lengths 
  , fp.* 
  -- flag each conversion session 
  , CASE WHEN fp.heap_session_conversion_cnt > 0 THEN 1 ELSE 0 END AS heap_session_is_conversion

FROM {{ ref('src_heap_sessions_eph') }} s
LEFT JOIN funnel_pivot fp
    ON s.heap_user_id = fp.funnel_user_id
    AND s.heap_session_id = fp.funnel_session_id 