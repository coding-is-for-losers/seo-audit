SELECT
type,
value as filename,
count,
classification,
pct_of_classification,
pct_of_value
FROM
{{ ref('deepcrawl_classification_stats') }}
WHERE 
type = 'filename'