SELECT 
run_date,
account,
client,
platform,
url,
count(distinct(keyword)) as gsc_keyword_count_90d,
sum(impressions_90d) as gsc_impressions_90d,
sum(clicks_90d) as gsc_clicks_90d,	
sum(clicks_90d)/sum(impressions_90d) as gsc_ctr_90d,
sum(impressions_90d * pos_90d)/sum(impressions_90d) as gsc_pos_90d
FROM `curious-domain-121318`.`seo_audit`.`search_console_history`
GROUP BY run_date, account, client, platform, url