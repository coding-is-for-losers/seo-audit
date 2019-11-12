SELECT
site, 
domain,
account,
date,
unix_date,
url,
sum(impressions) impressions,
sum(clicks) clicks,
CASE WHEN sum(impressions) > 0 THEN sum(clicks) / sum(impressions) END as ctr,
CASE WHEN sum(impressions) > 0 THEN sum(average_position*impressions)/sum(impressions) ELSE null END as avg_position
FROM (

	SELECT
	a.site, 
	a.domain,
	a.account,
	date,
	unix_date,
	CASE WHEN b.url is not null THEN a.url
		ELSE regexp_replace(a.url, r'\?.*$', '') END as url,
		-- WHEN regexp_contains(a.url, r'^.*\/([^\/]+?\.[^\/]+)$') THEN regexp_replace(a.url, r'\?.*$', '')
  --       ELSE trim(regexp_replace(a.url, r'\?.*$',''),'/') END as url,        
	impressions,
	clicks,
	average_position
	FROM {{ ref('search_console_url_proc') }} a
    LEFT JOIN {{ ref('deepcrawl_stats')}} b
    ON (
        a.date = b.report_date AND 
        a.site = b.site AND 
    	a.url = b.url
    )        
)
GROUP BY site, domain, account, date, unix_date, url