SELECT
a.date date,
c.run_date run_date,
a.client client,
coalesce(found_at_sitemap, sitemap) sitemap,
a.url url,
domain,
canonical_url,
page_type,
page_objective,

#indicative actions roll up to each other, nested one by one

case when a.url like '%#%' then 'leave as is' else '' end as anchored_url_action,

case when page_type is null then 'missing from crawl' else '' end as crawl_action,

case 
	when http_status_code = 404 then concat('301 redirect to ', last_subfolder) 
	when http_status_code = 302 then concat('301 redirect to ', redirected_to_url)
	when http_status_code = 301 then 'leave as is'
	else '' end as http_status_action,

case 
	when ( robots_noindex = true or http_status_code in (404, 302, 301) ) and ( sitemap is not null or found_at_sitemap is not null ) then concat('remove from sitemap: ', coalesce(found_at_sitemap, sitemap) ) 
	when canonical_status = 'canonicalized' then 'canonicalized, leave as is'
	when ( robots_noindex = false or robots_noindex is null ) and sitemap is null and found_at_sitemap is null and a.url not like '%/page/%' then 'add to sitemap'
	else '' end as sitemap_action,

case 
	when a.gsc_top_url_for_keyword_90d != a.url and a.gsc_top_keyword_90d != '' and a.gsc_top_url_impressions_for_keyword_90d > 500 then concat('canonicalize to: ', a.gsc_top_url_for_keyword_90d)
	when b.gsc_top_url_for_keyword_90d != canonical_url and a.gsc_top_keyword_90d != '' and b.gsc_top_url_impressions_for_keyword_90d > 500 then concat('canonicalize to: ', b.gsc_top_url_for_keyword_90d)
	when canonical_status = 'missing_canonical' then 'missing canonical'
	when canonical_status = 'canonicalized' then 'canonicalized, leave as is'
	else '' end as canonical_action,		

case
	when page_objective in ('revenue', 'sales') then ''
	when first_subfolder_sessions_30d = 0 and first_subfolder_pageviews_30d > 0 and first_subfolder_http_status = '' then concat('block crawl to: ', first_subfolder)
	when second_subfolder_sessions_30d = 0 and second_subfolder_pageviews_30d > 0 and second_subfolder_http_status = ''  then concat('block crawl to: ', second_subfolder)
	when last_subfolder_sessions_30d = 0 and last_subfolder_pageviews_30d > 0 and last_subfolder_http_status = '' then concat('block crawl to: ', last_subfolder)
	when sessions_30d = 0 and pageviews_30d > 0 then 'noindex'
	when  and pageviews_30d = 0 and last_subfolder_pageviews_30d > 0 then concat('301 to: ', last_subfolder)
	when pageviews_30d = 0 and second_subfolder_pageviews_30d > 0 then concat('301 to: ', second_subfolder)
	when pageviews_30d = 0 and first_subfolder_pageviews_30d > 0 then concat('301 to: ', first_subfolder)
	when pageviews_30d = 0 and first_subfolder_pageviews_30d = 0 then concat('301 to: ', domain)
	else '' end as traffic_redirect_action,		

# analytics actions are separate from indicative actions - only display if admin_action in ('', 'add to sitemap', 'missing from crawl')

case 
	when page_objective = 'pageviews' and pageviews_30d = 0 then 'review content for relevance'
	when page_type in ('homepage', 'info', 'article') and sessions_30d > 0 and word_count < 2000 then 'review thin content'	
	when page_objective = 'revenue' and revenue_30d = 0 and sessions_30d > 0 then  'review relevance for top keywords'
	when page_objective = 'sales' and transactions_30d = 0 and sessions_30d > 0 then 'review relevance for top keywords'
	when page_objective in ('revenue', 'sales') and sessions_yoy_pct < 0 then 'review relevance for top keywords'
else '' end as content_action,

case 
	when page_objective = 'pageviews' and pageviews_30d = 0 and links_in_count < 10 then 'review low internal link count'
	when page_objective = 'revenue' and ( revenue_30d = 0 or sessions_yoy_pct < 0 ) then 'target external links'
	when page_objective = 'sales' and ( transactions_30d = 0 or sessions_yoy_pct < 0 ) then 'target external links'
else '' end as link_action,

case 
	when page_type is not null and (description is null or page_title is null) then 'metas missing' 
	when page_type is not null and (title_contains_top_keyword + description_contains_top_keyword) = 0 then concat('update metas to include top keyword: ', a.gsc_top_keyword_90d) 
	when page_type is not null and sessions_30d = 0 and a.gsc_top_keyword_impressions_90d > 0 then 'review meta relevance for top keywords'
	else '' end as meta_rewrite_action,

# category actions (only display pagination_action if category_action is blank)

case when page_type = 'blog_category' and page_type_rank > 10 and a.url not like '%/page/%' then concat('potential 301 to top category: ', first_value(a.url) over (partition by page_type order by page_type_rank asc)) else '' end as category_action,

case when page_type like '%category%' and flag_paginated = 0 and url_contains_digit = 1 then 'needs pagination' else '' end as pagination_action,

# push these schema classifications down into a lower proc model (and push other proc from deepcrawl up)
case when page_type = 'product' and schema_type not like '%product%' then 'product' else '' end as schema_product,
case when page_type = 'product' and schema_type not like '%itemavailability%' then 'itemavailability' else '' end as schema_itemavailability,
case when page_type = 'product' and schema_type not like '%rating%' then 'aggregaterating' else '' end as schema_aggregaterating,
case when page_type = 'product_category' and schema_type not like '%itemlistordertype%' then 'itemlistordertype' else '' end as schema_itemlistordertype,
case when page_type = 'article' and schema_type not like '%blogposting%' then 'blogposting' else '' end as schema_blogposting,
case when page_type = 'blog_category' and schema_type not like '%blog%' then 'blog' else '' end as schema_blog,
case when page_type = 'local' and schema_type not like '%localbusiness%' then 'localbusiness' else '' end as schema_localbusiness,
case when page_type = 'homepage' and schema_type not like '%organization%' then 'organization' else '' end as schema_organization,


page_type_rank,
url_stripped,
url_protocol,
canonical_url_protocol,
protocol_match,
protocol_count,
canonical_url_stripped,
canonical_status,
urls_to_canonical,
first_subfolder,
first_subfolder_http_status,
second_subfolder,
second_subfolder_http_status,
last_subfolder,
last_subfolder_http_status,
first_subfolder_sessions_30d,
first_subfolder_pageviews_30d,
second_subfolder_sessions_30d,
second_subfolder_pageviews_30d,
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
a.gsc_keyword_count_90d,
a.gsc_impressions_90d,
a.gsc_clicks_90d,	
a.gsc_ctr_90d,
a.gsc_top_keyword_90d,
a.gsc_top_url_for_keyword_90d,
a.gsc_top_url_impressions_for_keyword_90d,
b.gsc_top_url_for_keyword_90d gsc_top_canonical_url_for_keyword_90d,
b.gsc_top_url_impressions_for_keyword_90d gsc_top_canonical_url_impressions_for_keyword_90d,
a.gsc_top_keyword_impressions_90d, 
a.gsc_top_keyword_clicks_90d, 
a.gsc_top_keyword_ctr_90d,
semrush_keyword_count,
semrush_total_cpc,
semrush_total_search_volume,
semrush_min_position,
semrush_top_keyword_vol,
semrush_top_keyword_vol_vol, 
semrush_top_keyword_vol_cpc, 
semrush_top_keyword_vol_pos,
semrush_top_keyword_cpc,
semrush_top_keyword_cpc_vol, 
semrush_top_keyword_cpc_cpc,
semrush_top_keyword_cpc_pos		
FROM {{ ref('agg_all') }} a
LEFT JOIN {{ref('search_console_stats_keyword')}} b
ON (
	a.canonical_url = b.url AND
	a.date = b.run_date
	)
LEFT JOIN  {{ ref('dates') }} c
ON (
	a.client = c.client
)
WHERE ( a.date = c.run_date
OR a.date is null )
and ( sessions_30d > 0 or sessions_mom > 0 or sessions_yoy > 0 or page_type is not null )