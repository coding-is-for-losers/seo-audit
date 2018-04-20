SELECT
date, 
account, 
platform, 
url, 
semrush_top_keyword_vol,
semrush_top_keyword_cpc,
max(semrush_top_keyword_vol_vol) semrush_top_keyword_vol_vol, 
max(semrush_top_keyword_vol_cpc) semrush_top_keyword_vol_cpc, 
max(semrush_top_keyword_cpc_vol) semrush_top_keyword_cpc_vol, 
max(semrush_top_keyword_cpc_cpc) semrush_top_keyword_cpc_cpc
FROM 

(

SELECT  
date, 
account,
platform,
url,
first_value(keyword) OVER w1 as semrush_top_keyword_vol,
first_value(search_volume) OVER w1 as semrush_top_keyword_vol_vol,
first_value(cpc) OVER w1 as semrush_top_keyword_vol_cpc,
first_value(keyword) OVER w2 as semrush_top_keyword_cpc,
first_value(search_volume) OVER w2 as semrush_top_keyword_cpc_vol,
first_value(cpc) OVER w2 as semrush_top_keyword_cpc_cpc
FROM {{ref('semrush_keyword_proc')}}
where keyword not in {{var('brand_keywords')}}
WINDOW w1 AS (PARTITION BY account, url, date ORDER BY search_volume desc),
w2 AS (PARTITION BY account, url, date ORDER BY cpc desc)

)

GROUP BY date, account, platform, url, semrush_top_keyword_vol, semrush_top_keyword_cpc
ORDER BY url asc, date desc
