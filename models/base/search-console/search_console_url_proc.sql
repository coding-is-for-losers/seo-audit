select 
b.site, 
b.domain,
a.account,
date,
unix_date,
date_of_entry,
url,
sum(impressions) impressions,
sum(clicks) clicks,
CASE WHEN sum(impressions) > 0 THEN sum(clicks) / sum(impressions) END as ctr,
CASE WHEN sum(impressions) > 0 THEN sum(average_position*impressions)/sum(impressions) ELSE null END as avg_position
FROM (

	SELECT  
	month date, 
	unix_date(month) as unix_date,
	time_of_entry,
	cast(time_of_entry as date) date_of_entry,
	first_value(time_of_entry) OVER (PARTITION BY requested_object, landing_page, month ORDER BY time_of_entry desc) lv,	
	requested_object as account,
	lower(concat(trim(regexp_replace(regexp_replace(replace(replace(replace(landing_page,'www.',''),'http://',''),'https://',''),r'\?.*$',''),r'\#.*$',''),'/'),'/')) url,
	impressions,
	clicks,
	average_position
	FROM `{{ target.project }}.{{ target.schema }}.gsc`
	
	) a
LEFT JOIN {{ ref('domains_proc') }} b
ON (
	a.account = b.search_console_account
)
WHERE time_of_entry = lv
GROUP BY b.site, b.domain, a.account, date, unix_date, date_of_entry, url
