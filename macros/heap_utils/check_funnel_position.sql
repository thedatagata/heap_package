{% macro check_funnel_position(heap_event_name) %}
    CASE 
        {%- for event_name in var('funnel_events') %}
            WHEN {{heap_event_name}} = '{{event_name}}' THEN {{loop.index}}
        {% endfor %}
    ELSE 0 
    END
{% endmacro %}