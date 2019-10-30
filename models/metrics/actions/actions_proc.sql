SELECT
date,
crawl_date,
site,
domain,
url,
found_at,
found_at_sitemap,
canonical_url,
page_type,

#indicative actions roll up to each other, nested one by one

case
	when http_status_code = 404 AND backlink_count > 0 AND internal_links_in_count > 0  THEN '301 redirect + remove internal link from found_at_url' 
	when http_status_code = 404 AND backlink_count > 0 THEN '301 redirect'
	when http_status_code = 404 THEN 'remove internal link from found_at_url'
	when http_status_code = 302 THEN '301 redirect to redirected_to_url'
	when http_status_code = 301 and lower(redirect_chain) = 'true' THEN 'fix redirect chain'
	when http_status_code = 301 and lower(redirect_chain) = 'false' THEN 'leave 301 as is'
	when http_status_code = 404 THEN '404, investigate'
	else '' end as http_status_action,

case 
	when ( http_status_code != 200 or is_noindex = 1 ) and found_at_sitemap is not null then concat('remove from sitemap: ', coalesce(found_at_sitemap) ) 
	when is_noindex = 0 and found_at_sitemap is null and flag_paginated = 0 and url not like '%/page/%' and sessions_30d > 0 and http_status_code = 200 then 'likely add to sitemap'
	else '' end as sitemap_action,

case 
	when http_status_code in (301, 404) then ''
	when canonical_status = 'missing_canonical' and flag_paginated = 0 then 'missing canonical'
	when canonical_status = 'canonicalized' and flag_paginated = 0 then 'canonicalized, leave as is'
	when canonical_status = 'canonicalized' and flag_paginated = 1 then 'paginated, remove canonical if possible'
	else '' end as canonical_action,

case
	when http_status_code is null and sessions_ttm > 0 then 'missing from crawl, orphaned page (no internal links)'
	when http_status_code is null and sessions_30d = 0 and flag_paginated = 0 then 'page likely removed, missing from crawl and 0 traffic this month'
	-- when first_subfolder_sessions_30d = 0 then concat('block crawl to: ', first_subfolder)
	-- when second_subfolder_sessions_30d = 0 then concat('block crawl to: ', second_subfolder)
	-- when last_subfolder_sessions_30d = 0 then concat('block crawl to: ', last_subfolder)
	when sessions_ttm < 45 and sessions_30d = 0 and canonical_status != 'canonicalized' and is_noindex = 0 and flag_paginated = 0 then 'potential noindex, review content for relevance'
	else '' end as crawl_action,		

-- review page_type classification algo
# push these schema classifications down into a lower proc model (and push other proc from deepcrawl up)
case when flag_paginated = 1 then '' 
	when level >= 2 and schema_type not like '%breadcrumb%' then 'breadcrumb' 
	when page_type = 'product' and schema_type not like '%product%' then 'product'
	when page_type = 'product_category' and schema_type not like '%itemlistordertype%' then 'itemlistordertype'
	when page_type = 'article' and schema_type not like '%article%' then 'article'
	when page_type = 'blog_category' and schema_type not like '%blog%' then 'blog'
	when page_type = 'local' and schema_type not like '%localbusiness%' then 'localbusiness'
	when page_type = 'homepage' and schema_type not like '%organization%' then 'organization'
	else '' END as schema_action,


# analytics actions are separate from other actions - only display if admin_action in ('', 'add to sitemap', 'missing from crawl')

CASE WHEN http_status_code != 200 OR flag_paginated = 1 THEN '' 
	WHEN http_status_code = 200 AND main_keyword_cannibalization_flag = 1 AND best_keyword_cannibalization_flag = 1 AND main_keyword = best_keyword AND is_self_canonical = TRUE THEN 'page cannibalizing main keyword, consider consolidating content'
	WHEN http_status_code = 200 AND main_keyword_cannibalization_flag = 1 AND best_keyword_cannibalization_flag = 1 AND main_keyword != best_keyword AND is_self_canonical = TRUE THEN 'page cannibalizing main + best keywords, consider consolidating content'
	WHEN http_status_code = 200 AND main_keyword_cannibalization_flag = 1 AND best_keyword_cannibalization_flag = 0 AND is_self_canonical = TRUE THEN 'page cannibalizing main keyword, consider consolidating content'
	WHEN http_status_code = 200 AND main_keyword_cannibalization_flag = 0 AND best_keyword_cannibalization_flag = 1 AND is_self_canonical = TRUE THEN 'page cannibalizing best keyword, consider consolidating content'
	ELSE '' END as cannibalization_action,

CASE WHEN http_status_code != 200 OR flag_paginated = 1 THEN '' 
	WHEN http_status_code = 200 AND impressions_mom_pct > total_impressions_mom_pct and clicks_mom_pct > total_clicks_mom_pct THEN 'rising content'
	WHEN http_status_code = 200 AND transactions_30d > 0 and sessions_30d > med_sessions_30d and ecommerce_conversion_rate_mom_pct < total_organic_ecommerce_conversion_rate_mom_pct THEN 'below-trend conversion rate'
	WHEN http_status_code = 200 AND sessions_30d > med_sessions_30d and goal_conversion_rate_all_goals_mom_pct < total_organic_goal_conversion_rate_mom_pct THEN 'below-trend conversion rate'
	WHEN http_status_code = 200 AND sessions_30d < 5 and transactions_30d = 0 and goal_completions_all_goals_30d = 0 and backlink_count > 0 THEN 'potential 301: low traffic, has external links'
	WHEN http_status_code = 200 AND sessions_30d < 5 and transactions_30d = 0 and goal_completions_all_goals_30d = 0 and backlink_count = 0 THEN 'potential removal: low traffic, 0 conversions or backlinks'
	WHEN http_status_code = 200 AND page_type = 'lead generation' and transactions_30d = 0 and goal_completions_all_goals_30d = 0 THEN 'inactive lead gen page: 0 conversions'
	ELSE '' END as content_action,

CASE WHEN http_status_code != 200 OR flag_paginated = 1 THEN '' 
	WHEN http_status_code = 200 AND internal_links_out_count <= bottom_quartile_internal_links_out_count and ( sessions_30d > med_sessions_30d or ref_domain_count >= med_ref_domain_count ) THEN 'add internal outlinks'
	WHEN http_status_code = 200 AND internal_links_in_count <= bottom_quartile_internal_links_in_count and ( sessions_30d > med_sessions_30d or ref_domain_count >= med_ref_domain_count ) THEN 'add internal inlinks'
	WHEN http_status_code = 200 AND internal_links_in_count >= top_quartile_internal_links_in_count and ( sessions_30d <= bottom_quartile_sessions_30d or ref_domain_count <= bottom_quartile_ref_domain_count ) THEN 'reduce internal inlinks'
	ELSE '' END as internal_link_action,

CASE WHEN http_status_code != 200 OR flag_paginated = 1 THEN '' 
	WHEN http_status_code = 200 AND main_avg_position >= 3 and main_avg_position <= 20 THEN 'target external links'
	WHEN http_status_code = 200 AND ( top_20_keywords - top_3_keywords ) >= 3 THEN 'target external links'
	ELSE '' END as external_link_action,

CASE WHEN http_status_code != 200 OR flag_paginated = 1 THEN '' 
	WHEN http_status_code = 200 AND (description is null or page_title is null ) and http_status_code is not null then 'metas missing' 
	WHEN http_status_code = 200 AND (title_contains_top_keyword + description_contains_top_keyword) = 0 AND (best_impressions > 0 OR main_impressions > 0) then concat('update metas to include main or best keyword') 
	WHEN http_status_code = 200 AND impressions_mom_pct > total_impressions_mom_pct and ctr_30d < med_ctr_30d AND (best_impressions > 0 OR main_impressions > 0) THEN 'low ctr: review meta relevance for top keywords'
	else '' end as meta_rewrite_action,

# category actions (only display pagination_action if category_action is blank)

-- case when page_type = 'category' and page_type_rank > 10 and sessions_30d < 5 and url not like '%/page/%' then concat('potential 301 to top category: ', first_value(url) over (partition by page_type, domain, date order by page_type_rank asc)) else '' end as category_action,
'' as category_action,

case when page_type = 'category' and flag_paginated = 0 and url_contains_digit = 1 then 'needs pagination' else '' end as pagination_action,

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
second_subfolder,
last_subfolder,
first_subfolder_sessions_30d,
second_subfolder_sessions_30d,
last_subfolder_sessions_30d,
last_subfolder_canonical,
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
meta_noindex,
is_noindex,
is_self_canonical,
redirected_to_url,
found_at_url,
rel_next_url,
rel_prev_url,
flag_paginated,
internal_links_in_count,
bottom_quartile_internal_links_in_count,
top_quartile_internal_links_in_count,
links_out_count,
external_links_count,
internal_links_out_count,
bottom_quartile_internal_links_out_count,
h1_tag,
h2_tag,
redirect_chain,
redirected_to_status_code,
is_redirect_loop,
duplicate_page,
duplicate_page_count,
duplicate_body,
duplicate_body_count,

-- ga + gsc data
sessions_30d,
bottom_quartile_sessions_30d,
med_sessions_30d,
pct_of_organic_sessions_30d,
transaction_revenue_30d,
transactions_30d,
pct_of_organic_transactions_30d,
ecommerce_conversion_rate_30d,
med_transaction_conversion_rate_30d,
goal_completions_all_goals_30d,
pct_of_organic_goal_completions_all_goals_30d,
goal_conversion_rate_all_goals_30d,
med_goal_conversion_rate_30d,
blended_conversions_30d,
blended_conversion_rate_30d,
bounce_rate_30d,
avg_seconds_on_site_30d,
sessions_mom,
sessions_mom_pct,
total_organic_sessions_mom_pct,
transaction_revenue_mom,
transaction_revenue_mom_pct,
transactions_mom,
transactions_mom_pct,
ecommerce_conversion_rate_mom_pct,
total_organic_ecommerce_conversion_rate_mom_pct,
goal_completions_all_goals_mom,
goal_completions_all_goals_mom_pct,
goal_conversion_rate_all_goals_mom_pct,
total_organic_goal_conversion_rate_mom_pct,
sessions_yoy,
sessions_yoy_pct,
total_organic_sessions_yoy_pct,
transaction_revenue_yoy,
transaction_revenue_yoy_pct,
transactions_yoy,
transactions_yoy_pct,
ecommerce_conversion_rate_yoy_pct,
total_organic_ecommerce_conversion_rate_yoy_pct,
goal_completions_all_goals_yoy,
goal_completions_all_goals_yoy_pct,
goal_conversion_rate_all_goals_yoy_pct,
total_organic_goal_conversion_rate_yoy_pct,
sessions_ttm,
transaction_revenue_ttm,
transactions_ttm,
goal_completions_all_goals_ttm,
gaining_traffic_mom,
gaining_traffic_yoy,
backlink_count,
ref_domain_count,
med_ref_domain_count,
bottom_quartile_ref_domain_count,
impressions_30d,
total_impressions_30d,
clicks_30d,
total_clicks_30d,
ctr_30d,
med_ctr_30d,
total_ctr_30d,
avg_position_30d,
impressions_mom,
total_impressions_mom,
impressions_mom_pct,
total_impressions_mom_pct,
clicks_mom,
total_clicks_mom,
clicks_mom_pct,
total_clicks_mom_pct,
ctr_mom,
total_ctr_mom,
ctr_mom_pct,
total_ctr_mom_pct,
avg_position_mom,
impressions_yoy,
total_impressions_yoy,
impressions_yoy_pct,
total_impressions_yoy_pct,
clicks_yoy,
total_clicks_yoy,
clicks_yoy_pct,
total_clicks_yoy_pct,
ctr_yoy,	
total_ctr_yoy,
ctr_yoy_pct,
total_ctr_yoy_pct,
avg_position_yoy,	
top_3_keywords,
top_5_keywords,
top_10_keywords,
top_20_keywords,
main_keyword,
main_impressions,
main_clicks,
main_avg_position,
main_top_url,
main_keyword_cannibalization_flag,
main_top_url_clicks,
best_keyword,
best_impressions,
best_clicks,
best_avg_position,
best_top_url,
best_top_url_clicks,
best_keyword_cannibalization_flag
FROM {{ ref('agg_all') }}
-- WHERE ( sessions_30d > 0 or sessions_mom > 0 or sessions_yoy > 0 or page_type is not null )