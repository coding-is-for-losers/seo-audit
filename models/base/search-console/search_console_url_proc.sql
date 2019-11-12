SELECT
b.site, 
b.domain,
a.account,
date,
unix_date,
date_of_entry,
url_untrimmed,
first_value(url_untrimmed) over (PARTITION BY site, domain, account, date, url_trimmed ORDER BY impressions desc) primary_url,
impressions,
clicks,
average_position
FROM (

	SELECT  
	month date, 
	unix_date(month) as unix_date,
	time_of_entry,
	cast(time_of_entry as date) date_of_entry,
	first_value(time_of_entry) OVER (PARTITION BY requested_object, landing_page, month ORDER BY time_of_entry desc) lv,	
	requested_object as account,
	lower(regexp_replace(replace(replace(replace(landing_page,'www.',''),'http://',''),'https://',''),r'\#.*$','')) url_untrimmed,
	trim(lower(regexp_replace(replace(replace(replace(landing_page,'www.',''),'http://',''),'https://',''),r'\#.*$','')),'/') url_trimmed,
	regexp_extract(landing_page,r'^(?:https?:\/\/)?(?:www\.)?([^\/]+)') as url_domain,
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
AND a.url_domain = b.domain

