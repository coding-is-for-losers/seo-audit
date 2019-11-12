SELECT
site, 
domain,
account,
date,
unix_date,
url,
keyword,
branded_flag,
sum(impressions) impressions,
sum(clicks) clicks,
CASE WHEN sum(impressions) > 0 THEN sum(clicks) / sum(impressions) END as ctr,
CASE WHEN sum(impressions) > 0 THEN sum(average_position*impressions)/sum(impressions) ELSE null END as avg_position,
sum(top_3_keywords) top_3_keywords,
sum(top_5_keywords) top_5_keywords,
sum(top_10_keywords) top_10_keywords,
sum(top_20_keywords) top_20_keywords
FROM (

	SELECT
	a.site, 
	a.domain,
	a.account,
	date,
	unix_date,
	CASE WHEN b.url is not null THEN a.url
		WHEN regexp_contains(a.url, r'^.*\/([^\/]+?\.[^\/]+)$') THEN regexp_replace(a.url, r'\?.*$', '')
        ELSE trim(regexp_replace(a.url, r'\?.*$',''),'/') END as url,
    keyword,
    branded_flag,
	impressions,
	clicks,
	average_position,
	top_3_keywords,
	top_5_keywords,
	top_10_keywords,
	top_20_keywords
	FROM {{ ref('search_console_keyword_proc') }} a
    LEFT JOIN {{ ref('deepcrawl_stats')}} b
    ON (
        a.date = b.report_date AND 
        a.site = b.site AND 
        ( a.url = b.url OR trim(a.url,'/') = b.url ) 
    )
)
GROUP BY site, domain, account, date, unix_date, url, keyword, branded_flag