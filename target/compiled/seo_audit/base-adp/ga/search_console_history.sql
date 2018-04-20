SELECT  
date, 
account,
platform,
url,
keyword,
sum(impressions) OVER w1 as impressions_90d,
sum(clicks) OVER w1 as clicks_90d,
sum(clicks)/sum(impressions) OVER w1 as ctr_90d
FROM `curious-domain-121318`.`seo_audit`.`search_console_proc`
WINDOW w1 AS (PARTITION BY account, url, keyword ORDER BY unix_date asc RANGE BETWEEN 90 PRECEDING AND CURRENT ROW)