SELECT
type,
value as first_path,
count,
classification,
pct_of_classification,
pct_of_value
FROM
{{ ref('deepcrawl_classification_stats') }}
WHERE 
type = 'first_path'
and pct_of_classification > .9
and pct_of_classification != 1
and pct_of_value != 1
