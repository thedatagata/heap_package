{% macro get_active_users(materialized_table_time) %}

    SELECT DISTINCT p.user_id 
    FROM {{ source('heap', 'all_events')}} e
    {% if is_incremental() %} 
        WHERE e.heap_event_time > ( SELECT MAX(t.{{ materialized_table_time }}) FROM {{this}} t )
    {% endif %}
    
{% endmacro %}