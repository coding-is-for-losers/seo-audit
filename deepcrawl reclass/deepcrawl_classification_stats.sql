with first_path as (
	
	SELECT
	'first_path' as type,
	first_path as value,
	classification,
	count(url) count
	FROM 
	{{ref('deepcrawl_class')}}
	GROUP BY 
	first_path, classification
),

filename as (
	SELECT
	'filename' as type,
	filename as value,
	classification,
	count(url) count
	FROM 
	{{ref('deepcrawl_class')}}
	GROUP BY 
	filename, classification

),

query_string as (
	SELECT
	'query_string' as type,
	query_string as value,
	classification,
	count(url) count
	FROM 
	{{ref('deepcrawl_class')}}
	GROUP BY 
	query_string, classification
)

SELECT 
type,
value,
count,
classification,
count/SUM(count) OVER (PARTITION BY classification, type) as pct_of_classification,
count/SUM(count) OVER (PARTITION BY value, type) as pct_of_value
FROM (

	SELECT * FROM first_path
	UNION ALL
	SELECT * FROM filename
	UNION ALL
	SELECT * FROM query_string

)
