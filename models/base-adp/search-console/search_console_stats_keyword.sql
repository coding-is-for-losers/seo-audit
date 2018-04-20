SELECT
run_date, 
account, 
platform, 
url, 
keyword gsc_top_keyword_90d, 
first_value(url) OVER w2 as gsc_top_url_for_keyword_90d,
first_value(max(impressions)) OVER w2 as gsc_top_url_impressions_for_keyword_90d,
max(impressions) gsc_top_keyword_impressions_90d, 
max(clicks) gsc_top_keyword_clicks_90d, 
max(ctr) gsc_top_keyword_ctr_90d
FROM 

(

SELECT  
run_date, 
account,
platform,
url,
first_value(keyword) OVER w1 as keyword,
first_value(impressions_90d) OVER w1 as impressions,
first_value(clicks_90d) OVER w1 as clicks,
first_value(ctr_90d) OVER w1 as ctr
FROM {{ref('search_console_history')}}
where keyword not in {{var('brand_keywords')}}
WINDOW w1 AS (PARTITION BY account, url ORDER BY impressions_90d desc)

)

GROUP BY run_date, account, platform, url, gsc_top_keyword_90d
WINDOW w2 AS (PARTITION BY account, keyword ORDER BY max(impressions) desc)
