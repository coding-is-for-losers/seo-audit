SELECT  
date, 
account,
platform,
url,
count(keyword) OVER w2 as gsc_keyword_count_90d,
sum(clicks_90d) OVER w1 as gsc_clicks_90d,
sum(impressions_90d) OVER w1 as gsc_impressions_90d,
sum(clicks_90d)/sum(impressions_90d) OVER w1 as gsc_ctr_90d,
first_value(keyword) OVER w2 as gsc_top_keyword_90d,
first_value(impressions_90d) OVER w2 as gsc_top_keyword_impressions_90d,
first_value(clicks_90d) OVER w2 as gsc_top_keyword_clicks_90d,
first_value(ctr_90d) OVER w2 as gsc_top_keyword_ctr_90d
FROM `curious-domain-121318`.`seo_audit`.`search_console_history`
WINDOW w1 AS (PARTITION BY account, url ORDER BY unix_date asc RANGE BETWEEN 90 PRECEDING AND CURRENT ROW),
w2 AS (PARTITION BY account, url, date ORDER BY impressions_90d desc)