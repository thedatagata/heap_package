{% macro gen_conversion_events_list() %}
  (
    {%- for event_name in var('conversion_events') -%}
    '{{ event_name }}' {{ "," if not loop.last }}
    {%- endfor -%}
  )
{% endmacro %}