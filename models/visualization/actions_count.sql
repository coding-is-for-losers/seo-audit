SELECT
date,
site,
action_type,
action,
count(distinct(url)) url_count,
sum(pct_of_organic_sessions_30d) pct_of_organic_sessions_30d
FROM (

	SELECT
	date,
	site,
	'Technical' as action_type,
	top_admin_action as action,
	url,
	pct_of_organic_sessions_30d
	FROM {{ ref('actions_hierarchy') }}
	WHERE crawl_action not like '%removed%'
	AND top_admin_action != '' AND top_admin_action is not null

	UNION ALL

	SELECT
	date,
	site,
	'On-page' as action_type,
	schema_action as action,
	url,
	pct_of_organic_sessions_30d
	FROM {{ ref('actions_hierarchy') }}
	WHERE schema_action != '' AND schema_action is not null

	UNION ALL

	SELECT
	date,
	site,
	'On-page' as action_type,
	meta_rewrite_action as action,
	url,
	pct_of_organic_sessions_30d
	FROM {{ ref('actions_hierarchy') }}
	WHERE meta_rewrite_action != '' AND meta_rewrite_action is not null	

	UNION ALL

	SELECT
	date,
	site,
	'On-page' as action_type,
	content_action as action,
	url,
	pct_of_organic_sessions_30d
	FROM {{ ref('actions_hierarchy') }}
	WHERE content_action != '' AND content_action is not null	
	AND content_action not like 'rising%'

	UNION ALL

	SELECT
	date,
	site,
	'Off-page' as action_type,
	external_link_action as action,
	url,
	pct_of_organic_sessions_30d
	FROM {{ ref('actions_hierarchy') }}
	WHERE external_link_action != '' AND external_link_action is not null		

	UNION ALL

	SELECT
	date,
	site,
	'Architecture' as action_type,
	cannibalization_action as action,
	url,
	pct_of_organic_sessions_30d
	FROM {{ ref('actions_hierarchy') }}	
	WHERE cannibalization_action != '' AND cannibalization_action is not null

	UNION ALL

	SELECT
	date,
	site,
	'Architecture' as action_type,
	internal_link_action as action,
	url,
	pct_of_organic_sessions_30d
	FROM {{ ref('actions_hierarchy') }}		
	WHERE internal_link_action != '' AND internal_link_action is not null

	UNION ALL

	SELECT
	date,
	site,
	'Architecture' as action_type,
	category_action as action,
	url,
	pct_of_organic_sessions_30d
	FROM {{ ref('actions_hierarchy') }}		
	WHERE category_action != '' AND category_action is not null	
)
GROUP BY date, site, action_type, action