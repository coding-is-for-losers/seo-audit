SELECT
date,
client,
url,
sitemap,
domain,
canonical_url,
canonical_status,
page_type,
page_objective,
case 
	when anchored_url_action != '' then anchored_url_action
	when crawl_action != '' then crawl_action
	when http_status_action != '' then http_status_action
	when sitemap_action != '' then sitemap_action
	when canonical_action != '' then canonical_action
	when traffic_redirect_action != '' then traffic_redirect_action
	when category_action != '' then category_action
	else '' end as admin_action,

case 
	when anchored_url_action != '' then 'anchored url'
	when crawl_action != '' then 'not crawled by deepcrawl'
	when http_status_action != '' then cast(http_status_code as string)
	when sitemap_action like '%remove%' then '301, 404 or noindexed page'
	when sitemap_action like '%leave as is%' then 'already canonicalized'
	when sitemap_action != '' then 'page missing from sitemap'
	when canonical_action like 'canonicalize to:%' then 'url has higher search impressions for top keyword'
	when canonical_action = 'missing canonical' then 'canonical url not found by deepcrawl'
	when canonical_action like '%leave as is%' then 'already canonicalized'
	when traffic_redirect_action like 'block crawl to:%' then 'subfolder receives no traffic'
	when traffic_redirect_action = 'noindex' then 'page receives pageviews but no organic traffic'
	when traffic_redirect_action like '301 to:%' then 'page receives no pageviews, 301 to subfolder'
	when category_action != '' then 'ranks below the top 10 blog category pages'
	else '' end as admin_action_reason,
 
# analytics actions are separate from indicative actions - only display if admin_action in ('', 'add to sitemap', 'missing from crawl')
content_action,
link_action,
meta_rewrite_action,
pagination_action,
first_subfolder,
first_subfolder_http_status,
second_subfolder,
second_subfolder_http_status,
last_subfolder,
last_subfolder_http_status,
http_status_code,
level,
schema_type,
header_content_type,
word_count, 
page_title,
title_contains_top_keyword,
page_title_length,
description,
description_contains_top_keyword,
description_length,
robots_noindex,
redirected_to_url,
links_in_count,
links_out_count,
h1_tag,
h2_tag,
redirect_chain,
redirected_to_status_code,
is_redirect_loop,
duplicate_page,
duplicate_page_count,
duplicate_body,
duplicate_body_count,
sessions_30d,
revenue_30d,
transactions_30d,
pageviews_30d,
#lead_conversion_rate_30d,
transaction_conversion_rate_30d,
#med_lead_conversion_rate_30d,
med_transaction_conversion_rate_30d,
sessions_mom,
revenue_mom,
transactions_mom,
pageviews_mom,
sessions_mom_pct,
revenue_mom_pct,
transactions_mom_pct,
sessions_yoy,
revenue_yoy,
transactions_yoy,
pageviews_yoy,
sessions_yoy_pct,
revenue_yoy_pct,
transactions_yoy_pct,
gaining_traffic_mom,
gaining_traffic_yoy,
ref_domain_count,
med_ref_domain_count,
avg_trust_flow,
avg_citation_flow,
gsc_keyword_count_90d,
gsc_impressions_90d,
gsc_clicks_90d,	
gsc_ctr_90d,
gsc_top_keyword_90d,
gsc_top_url_for_keyword_90d,
gsc_top_canonical_url_for_keyword_90d,
gsc_top_keyword_impressions_90d, 
gsc_top_keyword_clicks_90d, 
gsc_top_keyword_ctr_90d,
semrush_keyword_count,
semrush_total_cpc,
semrush_total_search_volume,
semrush_top_keyword_vol,
semrush_top_keyword_vol_vol, 
semrush_top_keyword_vol_cpc, 
semrush_top_keyword_cpc,
semrush_top_keyword_cpc_vol, 
semrush_top_keyword_cpc_cpc	
FROM {{ ref('actions_proc') }}