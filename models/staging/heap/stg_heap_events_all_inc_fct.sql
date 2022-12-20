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
, e.heap_session_event_sequence
, e.heap_defined_prop_channel_attribution
, e.heap_event_prop_logged_in
, {{check_funnel_position('e.heap_event_name')}} as heap_event_funnel_position
, e.heap_event_name IN {{ gen_conversion_event_list() }} as heap_is_conversion_event

FROM {{ ref('src_heap_events_all_eph') }} e