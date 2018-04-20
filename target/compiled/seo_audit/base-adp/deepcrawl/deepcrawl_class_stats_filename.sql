SELECT
type,
value as filename,
count,
classification,
pct_of_classification,
pct_of_value
FROM
`curious-domain-121318`.`seo_audit`.`deepcrawl_classification_stats`
WHERE 
type = 'filename'