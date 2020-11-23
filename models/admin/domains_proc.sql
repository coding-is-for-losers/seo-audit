SELECT
site,
domain,
search_console_account,
google_analytics_account
FROM (

	SELECT
	site_name site,
	trim(replace(replace(replace(full_homepage_url,'www.',''),'http://',''),'https://',''),'/') domain,
	google_search_console_account search_console_account,
	google_analytics_account,
	time_of_entry,
	first_value(time_of_entry) OVER (PARTITION BY site_name ORDER BY time_of_entry DESC) lv
	FROM `{{ target.project }}.{{ target.schema }}.sites`
	WHERE site_name is not null
)
WHERE lv = time_of_entry