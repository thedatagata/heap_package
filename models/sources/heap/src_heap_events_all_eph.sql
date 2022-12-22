WITH 
    all_events_base
        AS 
            (
                -- abstracted the incremental logic away from the incrementally materialized tables found within the staging directory
                -- creates a handoff point from an architect or data eng to analysts that are tasked with building dashboards or running analysis 
                SELECT * 
                FROM {{source('heap', 'all_events')}} e
                {% if is_incremental() %}
                    WHERE e.heap_user_id IN ( {{get_active_users('heap_event_time')}} )
                {% endif %}
            ), 
    all_events_deduped
        AS
                -- https://github.com/dbt-labs/dbt-utils#deduplicate-source
            (
                {{ dbt_utils.deduplicate(
                    relation='all_events_base',
                    partition_by='heap_user_id, heap_event_id',
                    order_by="heap_event_time desc",
                )
                }}
            )
SELECT * 
      -- generate the order in which each event that has been synced from heap occurred in within each session for each user
    , ROW_NUMBER() OVER( PARTITION BY e.heap_user_id, e.heap_session_id ORDER BY e.heap_event_time) as heap_session_event_sequence
FROM all_events_deduped e