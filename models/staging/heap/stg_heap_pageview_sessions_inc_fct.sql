
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
                  , LAG(EXTRACT(EPOCH FROM p.heap_pageview_time),1) OVER(PARTITION BY p.heap_user_id, p.heap_pageview_source ORDER BY p.heap_pageview_time) AS pageview_previous_epoch
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

    session_start_flag
        AS 
            (
                -- if the time between pageviews is greater than 30 minutes for web or 5 minutes for mobile, mark that pageview row with a 1
                SELECT 
                    pd.* 
                  , CASE 
                        WHEN pd.pageview_epoch_delta > (60 * 30) 
                        THEN 1 
                        ELSE 0 
                        END AS heap_pageview_session_start_flag 
                FROM pageviews_delta pd 
            ), 
    
    session_index 
        AS 
            (
                -- now we can sum up all of those 1s to get the session sequence number 
                SELECT
                    ss.*
                  , SUM(ss.heap_pageview_session_start_flag) OVER (PARTITION BY ss.heap_user_id ORDER BY ss.heap_pageview_time) AS heap_pageview_session_index 
                FROM session_start_flag ss
            ),
    
    sessions_base
        AS 
            (
                -- for every session sequence number, we want info about the first and last event in that session so that we can join events to sessions based on session start and end timestamps
                SELECT 
                    si.*
                  , ROW_NUMBER() OVER ( PARTITION BY si.heap_user_id, si.heap_pageview_session_index ORDER BY si.heap_pageview_time ) AS heap_pageview_session_sequence_number
                  , FIRST_VALUE(si.heap_pageview_time) OVER( PARTITION BY si.heap_user_id, si.heap_pageview_session_index ORDER BY si.heap_pageview_time) AS heap_pageview_session_start_time
                  , LAST_VALUE(si.heap_pageview_time) OVER (PARTITION BY si.heap_user_id, si.heap_pageview_session_index ORDER BY si.heap_pageview_time) AS heap_pageview_session_end_time
                  , FIRST_VALUE(si.heap_pageview_id) OVER (PARTITION BY si.heap_user_id, si.heap_pageview_session_index ORDER BY si.heap_pageview_time) AS heap_pageview_session_first_pageview_id
                  , LAST_VALUE(si.heap_pageview_id) OVER (PARTITION BY si.heap_user_id, si.heap_pageview_session_index ORDER BY si.heap_pageview_time) AS heap_pageview_session_last_pageview_id
                  , FIRST_VALUE(EXTRACT(EPOCH from si.heap_pageview_time)) OVER (PARTITION BY si.heap_user_id, si.heap_pageview_session_index ORDER BY si.heap_pageview_time) AS heap_pageview_session_start_epoch
                  , LAST_VALUE(EXTRACT(EPOCH from si.heap_pageview_time)) OVER (PARTITION BY si.heap_user_id, si.heap_pageview_session_index ORDER BY si.heap_pageview_time) AS heap_pageview_session_end_epoch
                FROM session_index si 
            )

-- update column names and add call to gen_defined_channel_attribution
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
  -- create a session_id concatinating user_id and session sequence number 
  , CONCAT(s.heap_user_id, '_', s.heap_pageview_session_index) heap_pageview_session_id
  , s.heap_pageview_session_index
  , s.heap_pageview_session_start_time
  , s.heap_pageview_session_end_time 
  , s.heap_pageview_session_first_pageview_id 
  , s.heap_pageview_session_last_pageview_id 
  , s.heap_pageview_session_start_epoch 
  , s.heap_pageview_session_end_epoch
  -- look forward session end time based on source of first event in session
  , DATEADD('m', 30, s.heap_session_end_time) as heap_session_lookforward_time

FROM sessions_base s