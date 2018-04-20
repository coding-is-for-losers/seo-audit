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