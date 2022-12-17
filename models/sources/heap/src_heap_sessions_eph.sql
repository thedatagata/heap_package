WITH 
    sessions_base 
        AS 
            (
                SELECT * 
                FROM {{source('heap','sessions')}} s 
                {% if is_incremental() %}
                    WHERE s.heap_user_id IN ( {{get_active_users('heap_session_start_time')}} )
                {% endif %}
            ), 
    sessions_deduped
        AS 
            (
                {{ dbt_utils.deduplicate(
                    relation='sessions_base',
                    partition_by='heap_user_id, heap_session_id',
                    order_by="heap_session_start_time desc",
                )
                }}
            )

SELECT
      s.*
    , ROW_NUMBER() OVER(PARTITION BY s.heap_user_id ORDER BY s.heap_session_start_time) AS heap_session_sequence_num
    , {{ gen_defined_channel_attribution('s.heap_session_utm_source', 's.heap_session_utm_medium', 's.heap_session_referrer', 's.heap_session_landing_page_query', 'chubbies') }} as heap_defined_channel_attribution

FROM sessions_deduped s