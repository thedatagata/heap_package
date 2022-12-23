{{
    config(
        unique_key='heap_event_id'
    )
}}

SELECT 
  e.heap_event_id 
, e.heap_user_id 
, e.heap_session_id 
, e.heap_event_name 
, e.heap_event_time
, e.heap_event_session_sequence
-- this checks each heap_event_name against the variable funnel_events defined in the dbt_project yml file and returns the index position within that list if there is a match
, {{check_funnel_position('e.heap_event_name')}} as heap_event_funnel_position
-- flag each heap_event_name that is found within the conversion_events list as defined in the dbt_project yml file 
, e.heap_event_name IN {{ gen_conversion_events_list() }} as heap_event_is_conversion

FROM {{ ref('src_heap_events_all_eph') }} e 