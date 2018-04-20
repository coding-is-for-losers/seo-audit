SELECT
type,
value as query_string,
count,
classification,
pct_of_classification,
pct_of_value
FROM
{{ ref('deepcrawl_classification_stats') }}
WHERE 
type = 'query_string'
and pct_of_classification > {{ var('deepcrawl_pct_of_classification_rule') }} 
and pct_of_classification != 1
and pct_of_value != 1
