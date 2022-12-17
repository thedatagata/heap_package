{% macro gen_defined_channel_attribution(utm_source, utm_medium, referrer, landing_page_query, sb_store_name) %}
    CASE
        WHEN {{utm_medium}} like '%cpc%' 
          AND {{utm_source}} like 'facebook'
            THEN 'Facebook Paid'

        WHEN {{utm_source}} like '%bing%' AND {{utm_medium}} like '%cpc%'
            THEN 'Paid Bing Search'

        WHEN ({{utm_medium}} like '%cpc%' OR {{landing_page_query}} like '%gclid%') AND {{referrer}} like '%google%'
            THEN 'Paid Google Search'
        
        WHEN {{referrer}} IS NULL 
            THEN 'Direct'

        WHEN ({{referrer}} like '%google%' OR {{referrer}} like '%bing%' OR {{referrer}} like '%yahoo%' OR {{referrer}} like '%duckduck%') 
          AND {{utm_medium}} IS NULL 
          AND {{utm_source}} IS NULL 
            THEN 'Organic Search'

        WHEN {{referrer}} like '%{{sb_store_name}}%'
            THEN '{{sb_store_name}}'
    
        ELSE 'Unattributed'

{% endmacro %}