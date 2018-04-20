SELECT  
b.run_date run_date,
b.unix_run_date unix_run_date,
account,
a.client,
platform,
url,
keyword,
sum(impressions) as impressions_90d,
sum(clicks) as clicks_90d,
sum(clicks)/sum(impressions) as ctr_90d,
sum(impressions * position)/sum(impressions) as pos_90d
FROM {{ref('search_console_proc')}} a
LEFT JOIN {{ ref('dates') }} b
ON (
	a.client = b.client
)
WHERE a.unix_date >= ( b.unix_run_date - 90 ) 
AND a.unix_date <= b.unix_run_date
group by run_date, unix_run_date, account, client, platform, url, keyword