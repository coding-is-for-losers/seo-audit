SELECT
a.date date,
c.run_date run_date,
a.client client,
sitemap,
found_at_sitemap,
a.url url,
domain,
canonical_url,
page_type,
case 
	when http_status_code = 404 then concat('301 redirect to ', last_subfolder) 
	when http_status_code = 302 then concat('301 redirect to ', redirected_to_url)
	when http_status_code = 301 then 'leave as is'
	else '' end as http_status_action,
case when a.url like '%#%' then 'leave as is' else '' end as anchored_url_action,
case when robots_noindex = true and sitemap is not null then concat('remove from sitemap: ', sitemap) 
	when ( robots_noindex = false or robots_noindex is null ) and sitemap is null then 'add to sitemap'
	else '' end as sitemap_action,
case when page_type is null then 'missing from crawl' end as crawl_action,
case 
	when page_type is not null and (description is null or page_title is null) then 'metas missing' 
	when page_type is not null and (title_contains_top_keyword + description_contains_top_keyword) = 0 then concat('update metas to include top keyword: ', a.gsc_top_keyword_90d) 
	when page_type is not null and ( a.gsc_top_keyword_90d is null or a.gsc_top_keyword_90d = '') then 'leave as is (no top keyword to include)'
	when page_type is not null then 'leave as is'
	else '' end as meta_rewrite_action,
case when page_type like '%category%' and flag_paginated = 0 then 'needs pagination' else '' end as pagination_action,
case 
	when a.gsc_top_url_for_keyword_90d != a.url and ( a.gsc_top_keyword_90d is not null or a.gsc_top_keyword_90d != '') then concat('canonicalize to ', a.gsc_top_url_for_keyword_90d)
	when b.gsc_top_url_for_keyword_90d != canonical_url and ( a.gsc_top_keyword_90d is not null or a.gsc_top_keyword_90d != '') then concat('canonicalize to ', b.gsc_top_url_for_keyword_90d)
	when canonical_status = 'missing_canonical' then 'missing canonical'
	when a.gsc_top_url_for_keyword_90d = a.url then 'leave as is' 
	else '' end as canonical_action,
case when page_type in ('homepage', 'info', 'article') and sessions_30d > 0 and word_count < 500 then 'update content' 
	when page_type in ('homepage', 'info', 'article') and sessions_30d > 0 and word_count >= 500 then 'leave as is'
	else '' end as thin_page_action,
case when page_type in ('homepage', 'info', 'article') and leads_30d > 0 or transactions_30d > 0 and sessions_yoy_pct < 0 then 'target links + on-page' else '' end as losing_traffic_action,
case when page_type = 'blog_category' and page_type_rank > 6 then 'quality review, potential 301 to top category' else '' end as category_action,
case when page_type = 'blog_category' then first_value(a.url) over (partition by page_type order by page_type_rank asc) else '' end as top_blog_category,
case 
	when page_type in ('404') then 'leave as is'
	when sessions_30d > 0 and a.gsc_top_keyword_impressions_90d >= 500 then 'leave as is'
	when sessions_30d > 0 and ( a.gsc_top_keyword_impressions_90d < 500 or a.gsc_top_keyword_impressions_90d is null ) then 'target links + on-page'
	when a.gsc_impressions_90d > 0 and sessions_30d = 0 then 'target links + on-page'
	when first_subfolder_sessions_30d = 0 and first_subfolder_pageviews_30d > 0 then concat('block crawl to: ', first_subfolder)
	when last_subfolder_sessions_30d = 0 and last_subfolder_pageviews_30d > 0 then concat('block crawl to: ', last_subfolder)
	when sessions_30d = 0 and pageviews_30d > 0 then 'noindex'
	when pageviews_30d = 0 and last_subfolder_pageviews_30d > 0 then concat('301 to: ', last_subfolder)
	when pageviews_30d = 0 and first_subfolder_pageviews_30d > 0 then concat('301 to: ', first_subfolder)
	when pageviews_30d = 0 and first_subfolder_pageviews_30d = 0 then concat('301 to: ', domain)
	else 'quality review' end as page_action,
page_type_rank,
url_stripped,
canonical_url_stripped,
canonical_status,
urls_to_canonical,
first_subfolder,
first_subfolder_sessions_30d,
first_subfolder_pageviews_30d,
last_subfolder,
last_subfolder_sessions_30d,
last_subfolder_pageviews_30d,
last_subfolder_canonical,
crawl_datetime,
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
indexable,
robots_noindex,
is_self_canonical,
backlink_count,
backlink_domain_count,
redirected_to_url,
found_at_url,
rel_next_url,
rel_prev_url,
flag_paginated,
links_in_count,
links_out_count,
external_links_count,
internal_links_count,
h1_tag,
h2_tag,
sessions_30d,
leads_30d,
transactions_30d,
pageviews_30d,
sessions_mom,
leads_mom,
transactions_mom,
pageviews_mom,
sessions_mom_pct,
leads_mom_pct,
transactions_mom_pct,
sessions_yoy,
leads_yoy,
transactions_yoy,
pageviews_yoy,
sessions_yoy_pct,
leads_yoy_pct,
transactions_yoy_pct,
gaining_traffic_mom,
gaining_traffic_yoy,
ref_domain_count,
avg_trust_flow,
avg_citation_flow,
a.gsc_keyword_count_90d,
a.gsc_impressions_90d,
a.gsc_clicks_90d,	
a.gsc_ctr_90d,
a.gsc_top_keyword_90d,
a.gsc_top_url_for_keyword_90d,
b.gsc_top_url_for_keyword_90d gsc_top_canonical_url_for_keyword_90d,
a.gsc_top_keyword_impressions_90d, 
a.gsc_top_keyword_clicks_90d, 
a.gsc_top_keyword_ctr_90d,
semrush_keyword_count,
semrush_total_cpc,
semrush_total_search_volume,
semrush_top_keyword_vol,
semrush_top_keyword_vol_vol, 
semrush_top_keyword_vol_cpc, 
semrush_top_keyword_cpc,
semrush_top_keyword_cpc_vol, 
semrush_top_keyword_cpc_cpc	
FROM `curious-domain-121318`.`seo_audit`.`agg_all` a
LEFT JOIN `curious-domain-121318`.`seo_audit`.`search_console_stats_keyword` b
ON (
	a.canonical_url = b.url AND
	a.date = b.run_date
	)
LEFT JOIN  `curious-domain-121318`.`seo_audit`.`dates` c
ON (
	a.client = c.client
)
WHERE ( a.date = c.run_date
OR a.date is null )
and ( sessions_30d > 0 or sessions_mom > 0 or sessions_yoy > 0 or page_type is not null )