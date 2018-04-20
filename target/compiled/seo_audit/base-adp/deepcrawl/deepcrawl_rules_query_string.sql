SELECT
type,
value as query_string,
count,
classification,
pct_of_classification,
pct_of_value
FROM
`curious-domain-121318`.`seo_audit`.`deepcrawl_classification_stats`
WHERE 
type = 'query_string'
and pct_of_classification > 0.9 
and pct_of_classification != 1
and pct_of_value != 1