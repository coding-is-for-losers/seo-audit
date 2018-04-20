SELECT 
date_in_range date, 
account, 
platform,
url,
'' as gsc_top_keyword_90d,
'' as gsc_top_url_for_keyword_90d,
null as gsc_top_url_impressions_for_keyword_90d,
'' as semrush_top_keyword_vol,
'' as semrush_top_keyword_cpc,
ref_domain_count,
avg_trust_flow,
avg_citation_flow,
null as gsc_keyword_count_90d,
null as gsc_impressions_90d,
null as gsc_clicks_90d,	
null as gsc_ctr_90d,
null as gsc_top_keyword_impressions_90d, 
null as gsc_top_keyword_clicks_90d, 
null as gsc_top_keyword_ctr_90d,
null as semrush_keyword_count,
null as semrush_total_cpc,
null as semrush_total_search_volume,
null as semrush_top_keyword_vol_vol, 
null as semrush_top_keyword_vol_cpc, 
null as semrush_top_keyword_cpc_vol, 
null as semrush_top_keyword_cpc_cpc,
null as sessions_30d,
null as revenue_30d,
null as transactions_30d,
null as pageviews_30d,
null as sessions_mom,
null as revenue_mom,
null as transactions_mom,
null as pageviews_mom,
null as sessions_yoy,
null as revenue_yoy,
null as transactions_yoy,
null as pageviews_yoy
FROM 
  `curious-domain-121318`.`seo_audit`.`majestic_domain_stats`
 UNION ALL

SELECT 
run_date date, 
account, 
platform,
url,
'' as gsc_top_keyword_90d,
'' as gsc_top_url_for_keyword_90d,
null as gsc_top_url_impressions_for_keyword_90d,
'' as semrush_top_keyword_vol,
'' as semrush_top_keyword_cpc,
null as ref_domain_count,
null as avg_trust_flow,
null as avg_citation_flow,
gsc_keyword_count_90d,
gsc_impressions_90d,
gsc_clicks_90d,	
gsc_ctr_90d,
null as gsc_top_keyword_impressions_90d, 
null as gsc_top_keyword_clicks_90d, 
null as gsc_top_keyword_ctr_90d,
null as semrush_keyword_count,
null as semrush_total_cpc,
null as semrush_total_search_volume,
null as semrush_top_keyword_vol_vol, 
null as semrush_top_keyword_vol_cpc, 
null as semrush_top_keyword_cpc_vol, 
null as semrush_top_keyword_cpc_cpc,
null as sessions_30d,
null as revenue_30d,
null as transactions_30d,
null as pageviews_30d,
null as sessions_mom,
null as revenue_mom,
null as transactions_mom,
null as pageviews_mom,
null as sessions_yoy,
null as revenue_yoy,
null as transactions_yoy,
null as pageviews_yoy
FROM 
  `curious-domain-121318`.`seo_audit`.`search_console_stats_url`
UNION ALL  

SELECT 
run_date date, 
account, 
platform,
url,
gsc_top_keyword_90d,
gsc_top_url_for_keyword_90d,
gsc_top_url_impressions_for_keyword_90d,
'' as semrush_top_keyword_vol,
'' as semrush_top_keyword_cpc,
null as ref_domain_count,
null as avg_trust_flow,
null as avg_citation_flow,
null as gsc_keyword_count_90d,
null as gsc_impressions_90d,
null as gsc_clicks_90d,	
null as gsc_ctr_90d,
gsc_top_keyword_impressions_90d, 
gsc_top_keyword_clicks_90d, 
gsc_top_keyword_ctr_90d,
null as semrush_keyword_count,
null as semrush_total_cpc,
null as semrush_total_search_volume,
null as semrush_top_keyword_vol_vol, 
null as semrush_top_keyword_vol_cpc, 
null as semrush_top_keyword_cpc_vol, 
null as semrush_top_keyword_cpc_cpc,
null as sessions_30d,
null as revenue_30d,
null as transactions_30d,
null as pageviews_30d,
null as sessions_mom,
null as revenue_mom,
null as transactions_mom,
null as pageviews_mom,
null as sessions_yoy,
null as revenue_yoy,
null as transactions_yoy,
null as pageviews_yoy
FROM 
  `curious-domain-121318`.`seo_audit`.`search_console_stats_keyword`
UNION ALL  

SELECT 
date_in_range date, 
account, 
platform,
url,
'' as gsc_top_keyword_90d,
'' gsc_top_url_for_keyword_90d,
null as gsc_top_url_impressions_for_keyword_90d,
'' as semrush_top_keyword_vol,
'' as semrush_top_keyword_cpc,
null as ref_domain_count,
null as avg_trust_flow,
null as avg_citation_flow,
null as gsc_keyword_count_90d,
null as gsc_impressions_90d,
null as gsc_clicks_90d,	
null as gsc_ctr_90d,
null as gsc_top_keyword_impressions_90d, 
null as gsc_top_keyword_clicks_90d, 
null as gsc_top_keyword_ctr_90d,
semrush_keyword_count,
semrush_total_cpc,
semrush_total_search_volume,
null as semrush_top_keyword_vol_vol, 
null as semrush_top_keyword_vol_cpc, 
null as semrush_top_keyword_cpc_vol, 
null as semrush_top_keyword_cpc_cpc,
null as sessions_30d,
null as revenue_30d,
null as transactions_30d,
null as pageviews_30d,
null as sessions_mom,
null as revenue_mom,
null as transactions_mom,
null as pageviews_mom,
null as sessions_yoy,
null as revenue_yoy,
null as transactions_yoy,
null as pageviews_yoy
FROM  
  `curious-domain-121318`.`seo_audit`.`semrush_url_stats`
UNION ALL  

SELECT 
date_in_range date, 
account, 
platform,
url,
'' as gsc_top_keyword_90d,
'' gsc_top_url_for_keyword_90d,
null as gsc_top_url_impressions_for_keyword_90d,
semrush_top_keyword_vol,
semrush_top_keyword_cpc,
null as ref_domain_count,
null as avg_trust_flow,
null as avg_citation_flow,
null as gsc_keyword_count_90d,
null as gsc_impressions_90d,
null as gsc_clicks_90d,	
null as gsc_ctr_90d,
null as gsc_top_keyword_impressions_90d, 
null as gsc_top_keyword_clicks_90d, 
null as gsc_top_keyword_ctr_90d,
null as semrush_keyword_count,
null as semrush_total_cpc,
null as semrush_total_search_volume,
semrush_top_keyword_vol_vol, 
semrush_top_keyword_vol_cpc, 
semrush_top_keyword_cpc_vol, 
semrush_top_keyword_cpc_cpc,
null as sessions_30d,
null as revenue_30d,
null as transactions_30d,
null as pageviews_30d,
null as sessions_mom,
null as revenue_mom,
null as transactions_mom,
null as pageviews_mom,
null as sessions_yoy,
null as revenue_yoy,
null as transactions_yoy,
null as pageviews_yoy
FROM  
  `curious-domain-121318`.`seo_audit`.`semrush_keyword_stats`

UNION ALL  

SELECT 
run_date date, 
account, 
platform,
url,
'' as gsc_top_keyword_90d,
'' gsc_top_url_for_keyword_90d,
null as gsc_top_url_impressions_for_keyword_90d,
'' as semrush_top_keyword_vol,
'' as semrush_top_keyword_cpc,
null as ref_domain_count,
null as avg_trust_flow,
null as avg_citation_flow,
null as gsc_keyword_count_90d,
null as gsc_impressions_90d,
null as gsc_clicks_90d,	
null as gsc_ctr_90d,
null as gsc_top_keyword_impressions_90d, 
null as gsc_top_keyword_clicks_90d, 
null as gsc_top_keyword_ctr_90d,
null as semrush_keyword_count,
null as semrush_total_cpc,
null as semrush_total_search_volume,
null as semrush_top_keyword_vol_vol, 
null as semrush_top_keyword_vol_cpc, 
null as semrush_top_keyword_cpc_vol, 
null as semrush_top_keyword_cpc_cpc,
sessions_30d,
revenue_30d,
transactions_30d,
pageviews_30d,
sessions_mom,
revenue_mom,
transactions_mom,
pageviews_mom,
sessions_yoy,
revenue_yoy,
transactions_yoy,
pageviews_yoy
FROM  
  `curious-domain-121318`.`seo_audit`.`ga_stats`