
{{
    config(
        unique_key='heap_pageview_id'
    )
}}

WITH 
    pageviews_lag
        AS 
            (
                -- have every pageview timestamp on the same row as the previous pageview
                SELECT
                    p.*
                  , EXTRACT(EPOCH FROM p.heap_pageview_time) as heap_pageview_epoch
                  , LAG(EXTRACT(EPOCH FROM p.heap_pageview_time),1) OVER(PARTITION BY p.heap_user_id ORDER BY p.heap_pageview_time) AS pageview_previous_epoch
                FROM {{ ref('src_heap_pageviews_eph') }} p  
            ),
    
    pageviews_delta 
        AS
            (
                -- calculate the delta between pageviews in seconds 
                SELECT 
                    pl.* 
                  , heap_pageview_epoch - pageview_previous_epoch AS pageview_epoch_delta
                FROM pageviews_lag pl 
                ORDER BY pl.heap_user_id, pl.heap_pageview_time
            ), 

    pageview_session_start_flag
        AS 
            (
                -- flag each pageview where the previous pageview occurred more than 30 minutes ago
                SELECT 
                    pd.* 
                  , CASE 
                        WHEN pd.pageview_epoch_delta > (60 * 30) 
                        THEN 1 
                        ELSE 0 
                        END AS heap_pageview_session_start_flag 
                FROM pageviews_delta pd 
            ), 
    
    pageview_session_index 
        AS 
            (
                -- for each user, get the rolling total of pageviews where the previous pageview occurred more than 30 minutes ago. this can then be used as a session_index number. 
                SELECT
                    ss.*
                  , SUM(ss.heap_pageview_session_start_flag) OVER (PARTITION BY ss.heap_user_id ORDER BY ss.heap_pageview_time) AS heap_pageview_session_index 
                FROM pageview_session_start_flag ss
            ),
    
    pageview_sessions_base
        AS 
            (
                -- for every session sequence number, we want info about the first and last event in that session so that we can join events to sessions based on session start and end timestamps
                SELECT 
                    si.*
                  -- determine the order in which each pageview occurred within each session 
                  , ROW_NUMBER() OVER ( PARTITION BY si.heap_user_id, si.heap_pageview_session_index ORDER BY si.heap_pageview_time ) AS heap_pageview_session_sequence_number
                  -- return the timestamp of the first pageview to occurr within each session
                  , FIRST_VALUE(si.heap_pageview_time) OVER( PARTITION BY si.heap_user_id, si.heap_pageview_session_index ORDER BY si.heap_pageview_time) AS heap_pageview_session_start_time
                  -- return the timestamp of the last pageview to occurr within each session 
                  , LAST_VALUE(si.heap_pageview_time) OVER (PARTITION BY si.heap_user_id, si.heap_pageview_session_index ORDER BY si.heap_pageview_time) AS heap_pageview_session_end_time
                  -- return the unique heap_pageview_id of the first pageview to occur within each session 
                  , FIRST_VALUE(si.heap_pageview_id) OVER (PARTITION BY si.heap_user_id, si.heap_pageview_session_index ORDER BY si.heap_pageview_time) AS heap_pageview_session_first_pageview_id
                  -- return the unique heap_pageview_id of the last pageview to occur within each session 
                  , LAST_VALUE(si.heap_pageview_id) OVER (PARTITION BY si.heap_user_id, si.heap_pageview_session_index ORDER BY si.heap_pageview_time) AS heap_pageview_session_last_pageview_id
                  -- return the epoch time of the first pageview to occur within each session 
                  -- this can be used to get to something like avg session duration
                  , FIRST_VALUE(EXTRACT(EPOCH from si.heap_pageview_time)) OVER (PARTITION BY si.heap_user_id, si.heap_pageview_session_index ORDER BY si.heap_pageview_time) AS heap_pageview_session_start_epoch
                  -- return the epoch time of the first pageview to occur within each session 
                  -- this can be used to get to something like avg session duration
                  , LAST_VALUE(EXTRACT(EPOCH from si.heap_pageview_time)) OVER (PARTITION BY si.heap_user_id, si.heap_pageview_session_index ORDER BY si.heap_pageview_time) AS heap_pageview_session_end_epoch
                FROM pageview_session_index si 
            )


SELECT 
    s.heap_pageview_id
  , s.heap_user_id
  , s.heap_session_id 
  , s.heap_pageview_time
  , s.heap_pageview_domain 
  , s.heap_pageview_path 
  , s.heap_pageview_query 
  , s.heap_pageview_hash 
  , s.heap_pageview_title 
  , s.heap_pageview_previous_page
  , s.gtm_user_data_visitor_type
  , s.gtm_user_data_total_order_count
  , s.gtm_user_data_cltv
  , s.gtm_user_data_is_new_user
  , s.gtm_cart_data_cart_value
    -- create a session_id concatinating user_id and session sequence number 
    -- https://github.com/dbt-labs/dbt-utils#generate_surrogate_key-source
  , {{ dbt_utils.generate_surrogate_key(['heap_user_id', 'heap_pageview_session_index'])}} AS heap_pageview_session_id 
  , s.heap_pageview_session_index 
  , s.heap_pageview_session_sequence_number
  , s.heap_pageview_session_start_time
  , s.heap_pageview_session_end_time 
  , s.heap_pageview_session_first_pageview_id 
  , s.heap_pageview_session_last_pageview_id 
  , s.heap_pageview_session_start_epoch 
  , s.heap_pageview_session_end_epoch

FROM pageview_sessions_base s