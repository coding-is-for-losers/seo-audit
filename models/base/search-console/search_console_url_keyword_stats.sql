-- depends_on: {{ ref('dates') }}, {{ ref('search_console_keyword_proc')}}
SELECT  
date,
account,
site,
domain,
url,
top_3_keywords,
top_5_keywords,
top_10_keywords,
top_20_keywords,
main_keyword,
main_impressions,
main_clicks,
main_avg_position,
first_value(url) OVER w3 as main_top_url,
first_value(main_clicks) OVER w3 as main_top_url_clicks,
best_keyword,
best_impressions,
best_clicks,
best_avg_position,
first_value(url) OVER w4 as best_top_url,
first_value(best_clicks) OVER w4 as best_top_url_clicks
FROM (

	SELECT 
	a.date, 
	unix_date, 
	b.unix_run_date,
	account,
	a.site,
	a.domain,
	url,
	sum(top_3_keywords) top_3_keywords,
	sum(top_5_keywords) top_5_keywords,
	sum(top_10_keywords) top_10_keywords,
	sum(top_20_keywords) top_20_keywords,
	max(main_keyword) as main_keyword,
	max(main_impressions) as main_impressions,
	max(main_clicks) as main_clicks,
	max(main_avg_position) as main_avg_position,
	max(best_keyword) as best_keyword,
	max(best_impressions) as best_impressions,
	max(best_clicks) as best_clicks,
	max(best_avg_position) as best_avg_position	
	FROM (

		SELECT 
		date, 
		unix_date, 
		account, 
		site, 
		domain,
		url, 
		top_3_keywords,
		top_5_keywords,
		top_10_keywords,
		top_20_keywords,
		first_value(keyword) OVER w1 as main_keyword,
		first_value(impressions) OVER w1 as main_impressions,
		first_value(clicks) OVER w1 as main_clicks,
		first_value(avg_position) OVER w1 as main_avg_position,
		first_value(keyword) OVER w2 as best_keyword,
		first_value(impressions) OVER w2 as best_impressions,
		first_value(clicks) OVER w2 as best_clicks,
		first_value(avg_position) OVER w2 as best_avg_position
		FROM {{ ref('search_console_keyword_proc') }} 
		WHERE impressions >= 50
		AND branded_flag = 0
		AND avg_position <= 25
		WINDOW w1 AS (PARTITION BY account, url, date ORDER BY impressions desc),
		w2 AS (PARTITION BY account, url, date ORDER BY avg_position asc)
	) a
	LEFT JOIN {{ ref('dates') }} b
	ON (
		a.site = b.site AND
		a.unix_date = b.unix_run_date
	)
	GROUP BY a.date, unix_date, b.unix_run_date, account, a.site, a.domain, url
)
WINDOW w3 as (PARTITION BY account, main_keyword, date ORDER BY main_clicks desc),
w4 as (PARTITION BY account, best_keyword, date ORDER BY best_clicks desc)
