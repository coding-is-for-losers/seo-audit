SELECT 
b.site, 
b.domain,
a.account,
date,
unix_date,
date_of_entry,
url,
keyword,
CASE WHEN regexp_contains(keyword, lower(site)) = TRUE THEN 1 ELSE 0 END as branded_flag,
sum(impressions) impressions,
sum(clicks) clicks,
sum(avg_position) avg_position,
max(CASE WHEN avg_position <= 3 THEN 1 ELSE 0 END) as top_3_keywords,
max(CASE WHEN avg_position <= 5 THEN 1 ELSE 0 END) as top_5_keywords,
max(CASE WHEN avg_position <= 10 THEN 1 ELSE 0 END) as top_10_keywords,
max(CASE WHEN avg_position <= 20 THEN 1 ELSE 0 END) as top_20_keywords
FROM (

	SELECT
	date,
	unix_date,
	time_of_entry,
	date_of_entry,
	account,
	lower(concat(trim(regexp_replace(regexp_replace(replace(replace(replace(landing_page,'www.',''),'http://',''),'https://',''),r'\?.*$',''),r'\#.*$',''),'/'),'/')) url,
	regexp_extract(landing_page,r'^(?:https?:\/\/)?(?:www\.)?([^\/]+)') as url_domain,
	keyword,
	impressions,
	clicks,
	avg_position
	FROM (

		SELECT  
		month date, 
		unix_date(month) as unix_date,
		time_of_entry,
		cast(time_of_entry as date) date_of_entry,
		first_value(time_of_entry) OVER (PARTITION BY requested_object, landing_page, search_query, month ORDER BY time_of_entry desc) lv,	
		requested_object as account,
		landing_page,
		search_query as keyword,
		impressions,
		clicks,
		average_position as avg_position
		FROM `{{ target.project }}.{{ target.schema }}.gsc_keywords`
		)
	WHERE time_of_entry = lv
	) a
LEFT JOIN {{ ref('domains_proc') }} b
ON (
	a.account = b.search_console_account
)
WHERE a.url_domain = b.domain
GROUP BY b.site, 
b.domain,
a.account,
date,
unix_date,
date_of_entry,
url,
keyword,
branded_flag
