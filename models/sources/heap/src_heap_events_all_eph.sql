WITH 
    all_events_base
        AS 
            (
                SELECT 
                    e.event_id AS heap_event_id 
                  , e.user_id AS heap_user_id 
                  , e.session_id AS heap_session_id 
                  , e.time AS heap_event_time 
                  , e.event_table_name AS heap_event_name 
                  
                FROM {{source('heap', 'all_events')}} e

                {% if is_incremental() %}
                    -- abstracted the incremental logic away from the incrementally materialized tables found within the staging directory
                    -- creates a handoff point from an architect or data eng to analysts that are tasked with building dashboards or running analysis 
                    -- use the column name that signifies the timestamp value of the staging model that references this ephemeral model 
                    WHERE e.user_id IN ( {{get_active_users('heap_event_time')}} )
                {% endif %}
            ),
    all_events_deduped
        AS 
            (
                -- https://github.com/dbt-labs/dbt-utils#deduplicate-source
                {{ dbt_utils.deduplicate(
                    relation='all_events_base e',
                    partition_by='e.heap_user_id, e.heap_event_id',
                    order_by='e.heap_event_time desc'
                )
                }}
            )
SELECT
      e.heap_event_id 
    , e.heap_user_id 
    , e.heap_session_id 
    , e.heap_event_time 
    , e.heap_event_name 
      -- generate the order in which each event that has been synced from heap occurred in within each session for each user
    , ROW_NUMBER() OVER( PARTITION BY e.heap_user_id, e.heap_session_id ORDER BY e.heap_event_time) as heap_event_session_sequence
FROM all_events_deduped e 