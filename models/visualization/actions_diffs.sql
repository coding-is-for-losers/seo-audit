SELECT
date,
crawl_date,
site,
domain,
url,
http_status_code,
level,
found_at,
found_at_url,
found_at_sitemap,
found_at_sitemap_prev,
CASE WHEN found_at_sitemap != found_at_sitemap_prev THEN 'Modified' ELSE 'No change' END as sitemap_diff,
CASE WHEN found_at_sitemap != found_at_sitemap_first THEN 'Modified' ELSE 'No change' END as sitemap_diff_first,
canonical_url,
canonical_url_prev,
CASE WHEN canonical_url != canonical_url_prev THEN 'Modified' ELSE 'No change' END as canonical_url_diff,
CASE WHEN canonical_url != canonical_url_prev THEN 'Modified' ELSE 'No change' END as canonical_url_diff_first,
canonical_status,
canonical_status_prev,
CASE WHEN canonical_status != canonical_status_prev THEN 'Modified' ELSE 'No change' END as canonical_status_diff,
CASE WHEN canonical_status != canonical_status_first THEN 'Modified' ELSE 'No change' END as canonical_status_diff_first,
page_type,
page_type_prev,
CASE WHEN page_type != page_type_prev THEN 'Modified' ELSE 'No change' END as page_type_diff,
CASE WHEN page_type != page_type_first THEN 'Modified' ELSE 'No change' END as page_type_diff_first,
admin_action_priority,
mktg_action_priority,
top_admin_action,
top_admin_action_reason,
top_admin_action_prev,
CASE WHEN top_admin_action_prev != '' and top_admin_action = '' THEN 'Fixed'
	WHEN top_admin_action_prev = '' and top_admin_action != '' AND http_status_action not like '%removed%' THEN 'New'
	WHEN top_admin_action_prev != '' and top_admin_action != '' and top_admin_action_prev != top_admin_action THEN 'Changed' 
	WHEN top_admin_action_prev = top_admin_action and top_admin_action != '' THEN 'Recurring' 
	ELSE 'None' END as top_admin_action_diff,
crawl_action,
crawl_action_priority,
crawl_action_prev,
CASE WHEN crawl_action_prev != '' and crawl_action = '' THEN 'Fixed'
	WHEN crawl_action_prev = '' and crawl_action != '' AND http_status_action not like '%removed%' THEN 'New'
	WHEN crawl_action_prev != '' and crawl_action != '' and crawl_action_prev != crawl_action THEN 'Changed' 
	WHEN crawl_action_prev = crawl_action AND crawl_action not like '%removed%' AND crawl_action != '' THEN 'Recurring' 
	ELSE 'None' END as crawl_action_diff,
http_status_action,
http_status_action_priority,
http_status_action_prev,
CASE WHEN http_status_action_prev != '' and http_status_action = '' THEN 'Fixed'
	WHEN http_status_action_prev = '' and http_status_action != '' THEN 'New'
	WHEN http_status_action_prev != '' and http_status_action != '' and http_status_action_prev != http_status_action THEN 'Changed' 
	WHEN http_status_action_prev = http_status_action AND http_status_action not like '%leave as is%' AND http_status_action != '' THEN 'Recurring' 
	ELSE 'None' END as http_status_action_diff,
sitemap_action,
sitemap_action_priority,
sitemap_action_prev,
CASE WHEN sitemap_action_prev != '' and sitemap_action = '' THEN 'Fixed'
	WHEN sitemap_action_prev = '' and sitemap_action != '' THEN 'New'
	WHEN sitemap_action_prev != '' and sitemap_action != '' and sitemap_action_prev != sitemap_action THEN 'Changed' 
	WHEN sitemap_action_prev = sitemap_action and sitemap_action != '' THEN 'Recurring' 
	ELSE 'None' END as sitemap_action_diff,
canonical_action,
canonical_action_priority,
canonical_action_prev,
CASE WHEN canonical_action_prev != '' and canonical_action = '' THEN 'Fixed'
	WHEN canonical_action_prev = '' and canonical_action != '' THEN 'New'
	WHEN canonical_action_prev != '' and canonical_action != '' and canonical_action_prev != canonical_action THEN 'Changed' 
	WHEN canonical_action_prev = canonical_action AND canonical_action not like '%leave as is%' and canonical_action != '' THEN 'Recurring' 
	ELSE 'None' END as canonical_action_diff,
schema_action,
schema_action_prev,
CASE WHEN schema_action_prev != '' and schema_action = '' THEN 'Fixed'
	WHEN schema_action_prev = '' and schema_action != '' THEN 'New'
	WHEN schema_action_prev != '' and schema_action != '' and schema_action_prev != schema_action THEN 'Changed' 
	WHEN schema_action_prev = schema_action and schema_action != '' THEN 'Recurring' 
	ELSE 'None' END as schema_action_diff,
on_off_page_action,
architecture_action,
cannibalization_action,
cannibalization_action_prev,
CASE WHEN cannibalization_action_prev != '' and cannibalization_action = '' THEN 'Fixed'
	WHEN cannibalization_action_prev = '' and cannibalization_action != '' THEN 'New'
	WHEN cannibalization_action_prev != '' and cannibalization_action != '' and cannibalization_action_prev != cannibalization_action THEN 'Changed' 
	WHEN cannibalization_action_prev = cannibalization_action and cannibalization_action != '' THEN 'Recurring' 
	ELSE 'None' END as cannibalization_action_diff,	
content_trajectory,
content_action,
content_action_prev,
CASE WHEN content_action_prev != '' and content_action = '' THEN 'Fixed'
	WHEN content_action_prev = '' and content_action != '' THEN 'New'
	WHEN content_action_prev != '' and content_action != '' and content_action_prev != content_action THEN 'Changed' 
	WHEN content_action_prev = content_action and content_action != '' THEN 'Recurring' 
	ELSE 'None' END as content_action_diff,
internal_link_action,
internal_link_action_prev,
CASE WHEN internal_link_action_prev != '' and internal_link_action = '' THEN 'Fixed'
	WHEN internal_link_action_prev = '' and internal_link_action != '' THEN 'New'
	WHEN internal_link_action_prev != '' and internal_link_action != '' and internal_link_action_prev != internal_link_action THEN 'Changed' 
	WHEN internal_link_action_prev = internal_link_action and internal_link_action != '' THEN 'Recurring' 
	ELSE 'None' END as internal_link_action_diff,
external_link_action,
external_link_action_prev,
CASE WHEN external_link_action_prev != '' and external_link_action = '' THEN 'Fixed'
	WHEN external_link_action_prev = '' and external_link_action != '' THEN 'New'
	WHEN external_link_action_prev != '' and external_link_action != '' and external_link_action_prev != external_link_action THEN 'Changed' 
	WHEN external_link_action_prev = external_link_action and external_link_action != '' THEN 'Recurring' 
	ELSE 'None' END as external_link_action_diff,
meta_rewrite_action,
meta_rewrite_action_prev,
CASE WHEN meta_rewrite_action_prev != '' and meta_rewrite_action = '' THEN 'Fixed'
	WHEN meta_rewrite_action_prev = '' and meta_rewrite_action != '' THEN 'New'
	WHEN meta_rewrite_action_prev != '' and meta_rewrite_action != '' and meta_rewrite_action_prev != meta_rewrite_action THEN 'Changed' 
	WHEN meta_rewrite_action_prev = meta_rewrite_action and meta_rewrite_action != '' THEN 'Recurring' 
	ELSE 'None' END as meta_rewrite_action_diff,			

is_noindex,
is_noindex_prev,
CASE WHEN is_noindex != is_noindex_prev THEN 1 ELSE 0 END as is_noindex_diff,
redirected_to_url,
redirected_to_url_prev,
CASE WHEN redirected_to_url != redirected_to_url_prev THEN 1 ELSE 0 END as redirect_diff,
-- schema changes

schema_type,
schema_type_prev,
schema_type_first,
CASE WHEN schema_type != schema_type_prev THEN 'Modified schema'
	ELSE 'No change' END as schema_type_diff, 
CASE WHEN schema_type != schema_type_first THEN 'Modified schema'
	ELSE 'No change' END as schema_type_diff_first, 	

-- meta changes
page_title,
page_title_prev,
page_title_first,
CASE WHEN page_title != page_title_prev THEN 1 ELSE 0 END as page_title_diff,
description,
description_prev,
description_first,
CASE WHEN page_title != page_title_prev AND description != description_prev THEN 'Modified title + description'
	WHEN page_title != page_title_prev THEN 'Modified title'
	WHEN description != description_prev THEN 'Modified description'
	ELSE 'No change' END as meta_title_description_diff, 
CASE WHEN page_title != page_title_first AND description != description_first THEN 'Modified title + description'
	WHEN page_title != page_title_first THEN 'Modified title'
	WHEN description != description_first THEN 'Modified description'
	ELSE 'No change' END as meta_title_description_diff_first, 

-- content changes
CASE WHEN new_content_flag = 1 THEN 'New page'
	ELSE 'Existing page' END as new_content_prev,
CASE WHEN new_content_flag_first >= 1 THEN 'New page'
	ELSE 'Existing page' END as new_content_first,		
CASE WHEN word_count_prev > 0 AND abs((word_count-word_count_prev)/word_count_prev) > .2
	AND (h1_tag != h1_tag_prev OR h2_tag != h2_tag_prev)
	THEN 'Modified word count + headers'
	WHEN word_count_prev > 0 AND abs((word_count-word_count_prev)/word_count_prev) > .2
	THEN 'Modfified word count'
	WHEN (h1_tag != h1_tag_prev OR h2_tag != h2_tag_prev)
	THEN 'Modified headers'
	ELSE 'No change' END as body_content_diff,
CASE WHEN word_count_first > 0 AND abs((word_count-word_count_first)/word_count_first) > .2
	AND (h1_tag != h1_tag_first OR h2_tag != h2_tag_first)
	THEN 'Modified word count + headers'
	WHEN word_count_first > 0 AND abs((word_count-word_count_first)/word_count_first) > .2
	THEN 'Modified word count'
	WHEN (h1_tag != h1_tag_first OR h2_tag != h2_tag_first)
	THEN 'Modified headers'
	ELSE 'No change' END as body_content_diff_first,	
word_count, 
word_count_prev, 
word_count_first, 
h1_tag,
h1_tag_prev,
h1_tag_first,
h2_tag,
h2_tag_prev,
h2_tag_first,

-- link changes
backlink_count,
backlink_count_prev,
backlink_count_first,
CASE WHEN backlink_count > backlink_count_prev THEN 'Gaining'
	WHEN backlink_count < backlink_count_prev THEN 'Losing'
	ELSE 'No change' END as backlink_count_diff,
CASE WHEN backlink_count > backlink_count_first THEN 'Gaining'
	WHEN backlink_count < backlink_count_first THEN 'Losing'
	ELSE 'No change' END as backlink_count_diff_first,	
ref_domain_count,
ref_domain_count_prev,
ref_domain_count_first,
ref_domain_count - ref_domain_count_prev as ref_domain_count_gained,
ref_domain_count - ref_domain_count_first as ref_domain_count_gained_first,
CASE WHEN ref_domain_count > ref_domain_count_prev THEN 'Gaining'
	WHEN ref_domain_count < ref_domain_count_prev THEN 'Losing'
	ELSE 'No change' END as ref_domain_count_diff,
CASE WHEN ref_domain_count > ref_domain_count_first THEN 'Gaining'
	WHEN ref_domain_count < ref_domain_count_first THEN 'Losing'
	ELSE 'No change' END as ref_domain_count_diff_first,	
internal_links_in_count,
internal_links_in_count_prev,
internal_links_in_count_first,
CASE WHEN internal_links_in_count > internal_links_in_count_prev THEN 'Gaining'
	WHEN internal_links_in_count < internal_links_in_count_prev THEN 'Losing'
	ELSE 'No change' END as internal_links_in_count_diff,
CASE WHEN internal_links_in_count > internal_links_in_count_first THEN 'Gaining'
	WHEN internal_links_in_count < internal_links_in_count_first THEN 'Losing'
	ELSE 'No change' END as internal_links_in_count_diff_first,	
internal_links_out_count,
internal_links_out_count_prev,
internal_links_out_count_first,
CASE WHEN internal_links_out_count > internal_links_out_count_prev THEN 'Gaining'
	WHEN internal_links_out_count < internal_links_out_count_prev THEN 'Losing'
	ELSE 'No change' END as internal_links_out_count_diff,
CASE WHEN internal_links_out_count > internal_links_out_count_first THEN 'Gaining'
	WHEN internal_links_out_count < internal_links_out_count_first THEN 'Losing'
	ELSE 'No change' END as internal_links_out_count_diff_first,	

-- analytics anomalies
top_3_keywords,
top_3_keywords_prev,
top_3_keywords_first,
top_3_keywords - top_3_keywords_prev as top_3_keywords_gained,
top_3_keywords - top_3_keywords_first as top_3_keywords_gained_first,
CASE WHEN top_3_keywords > top_3_keywords_prev THEN 'Gaining'
	WHEN top_3_keywords < top_3_keywords_prev THEN 'Losing'
	ELSE 'No change' END as top_3_keywords_diff,
CASE WHEN top_3_keywords > top_3_keywords_first THEN 'Gaining'
	WHEN top_3_keywords < top_3_keywords_first THEN 'Losing'
	ELSE 'No change' END as top_3_keywords_diff_first,	
top_10_keywords,
top_10_keywords_prev,
top_10_keywords_first,
top_10_keywords - top_10_keywords_prev as top_10_keywords_gained,
top_10_keywords - top_10_keywords_first as top_10_keywords_gained_first,
CASE WHEN top_10_keywords > top_10_keywords_prev THEN 'Gaining'
	WHEN top_10_keywords < top_10_keywords_prev THEN 'Losing'
	ELSE 'No change' END as top_10_keywords_diff,
CASE WHEN top_10_keywords > top_10_keywords_first THEN 'Gaining'
	WHEN top_10_keywords < top_10_keywords_first THEN 'Losing'
	ELSE 'No change' END as top_10_keywords_diff_first,	
top_20_keywords,
top_20_keywords_prev,
top_20_keywords_first,
top_20_keywords - top_20_keywords_prev as top_20_keywords_gained,
top_20_keywords - top_20_keywords_first as top_20_keywords_gained_first,
CASE WHEN top_20_keywords > top_20_keywords_prev THEN 'Gaining'
	WHEN top_20_keywords < top_20_keywords_prev THEN 'Losing'
	ELSE 'No change' END as top_20_keywords_diff,
CASE WHEN top_20_keywords > top_20_keywords_first THEN 'Gaining'
	WHEN top_20_keywords < top_20_keywords_first THEN 'Losing'
	ELSE 'No change' END as top_20_keywords_diff_first,		
sessions_30d,
sessions_ttm,
pct_of_organic_sessions_30d,
transaction_revenue_30d,
transactions_30d,
bounce_rate_30d,
avg_seconds_on_site_30d,
goal_completions_all_goals_30d,
pct_of_organic_transactions_30d,
impressions_30d,
clicks_30d,
avg_position_30d,
main_keyword,
main_top_url,
main_impressions,
main_clicks,
main_avg_position,
best_keyword,
best_top_url,
best_impressions,
best_clicks,
best_avg_position,
impressions_mom_pct,
ctr_mom_pct,
sessions_mom_pct,
sessions_yoy_pct
FROM (
	SELECT
	date,
	crawl_date,
	site,
	domain,
	url,
	http_status_code,
	admin_action_priority,
	mktg_action_priority,
	sitemap_action_priority,
	crawl_action_priority,
	http_status_action_priority,
	canonical_action_priority,
	found_at,
	found_at_sitemap,
	found_at_url,
	level,
	schema_type,
	lag(schema_type) over w1 as schema_type_prev,
	first_value(schema_type) over w1 as schema_type_first,
	lag(found_at_sitemap) over w1 as found_at_sitemap_prev,
	first_value(found_at_sitemap) over w1 as found_at_sitemap_first,
	canonical_url,
	lag(canonical_url) over w1 as canonical_url_prev,
	first_value(canonical_url) over w1 as canonical_url_first,
	canonical_status,
	lag(canonical_status) over w1 as canonical_status_prev,
	first_value(canonical_status) over w1 as canonical_status_first,
	page_type,
	lag(page_type) over w1 as page_type_prev,
	first_value(page_type) over w1 as page_type_first,
	top_admin_action,
	top_admin_action_reason,
	lag(top_admin_action) over w1 as top_admin_action_prev,	
	crawl_action,
	lag(crawl_action) over w1 as crawl_action_prev,
	http_status_action,
	lag(http_status_action) over w1 as http_status_action_prev,
	sitemap_action,
	lag(sitemap_action) over w1 as sitemap_action_prev,
	canonical_action,
	lag(canonical_action) over w1 as canonical_action_prev,
	schema_action,
	lag(schema_action) over w1 as schema_action_prev,
	on_off_page_action,
	architecture_action,
	# analytics actions are separate from indicative actions - only display if admin_action in ('', 'add to sitemap', 'missing from crawl')
	cannibalization_action,
	lag(cannibalization_action) over w1 as cannibalization_action_prev,
	content_trajectory,
	content_action,
	lag(content_action) over w1 as content_action_prev,
	internal_link_action,
	lag(internal_link_action) over w1 as internal_link_action_prev,
	external_link_action,
	lag(external_link_action) over w1 as external_link_action_prev,
	meta_rewrite_action,
	lag(meta_rewrite_action) over w1 as meta_rewrite_action_prev,

	-- noindex / redirect
	is_noindex,
	lag(is_noindex) over w1 as is_noindex_prev,
	redirected_to_url,
	lag(redirected_to_url) over w1 as redirected_to_url_prev,

	-- content changes
	word_count, 
	lag(word_count) over w1 as word_count_prev, 
	first_value(word_count) over w1 as word_count_first, 	
	page_title,
	lag(page_title) over w1 as page_title_prev,
	first_value(page_title) over w1 as page_title_first,	
	description,
	lag(description) over w1 as description_prev,
	first_value(description) over w1 as description_first,		
	h1_tag,
	lag(h1_tag) over w1 as h1_tag_prev,
	first_value(h1_tag) over w1 as h1_tag_first,	
	h2_tag,
	lag(h2_tag) over w1 as h2_tag_prev,
	first_value(h2_tag) over w1 as h2_tag_first,	

	-- link changes
	backlink_count,
	lag(backlink_count) over w1 as backlink_count_prev,
	first_value(backlink_count) over w1 as backlink_count_first,
	ref_domain_count,
	lag(ref_domain_count) over w1 as ref_domain_count_prev,
	first_value(ref_domain_count) over w1 as ref_domain_count_first,
	internal_links_in_count,
	lag(internal_links_in_count) over w1 as internal_links_in_count_prev,
	first_value(internal_links_in_count) over w1 as internal_links_in_count_first,
	internal_links_out_count,
	lag(internal_links_out_count) over w1 as internal_links_out_count_prev,
	first_value(internal_links_out_count) over w1 as internal_links_out_count_first,

	-- analytics anomalies
	top_3_keywords,
	lag(top_3_keywords) over w1 as top_3_keywords_prev,
	first_value(top_3_keywords) over w1 as top_3_keywords_first,
	top_10_keywords,
	lag(top_10_keywords) over w1 as top_10_keywords_prev,
	first_value(top_10_keywords) over w1 as top_10_keywords_first,
	top_20_keywords,
	lag(top_20_keywords) over w1 as top_20_keywords_prev,
	first_value(top_20_keywords) over w1 as top_20_keywords_first,
	
	new_content_flag,
	sum(new_content_flag) over w1 as new_content_flag_first,

	impressions_mom_pct,
	ctr_mom_pct,
	sessions_mom_pct,
	sessions_yoy_pct,

	-- are analytics necessary here?
	sessions_30d,
	sessions_ttm,
	pct_of_organic_sessions_30d,
	transaction_revenue_30d,
	transactions_30d,
	bounce_rate_30d,
	avg_seconds_on_site_30d,
	goal_completions_all_goals_30d,
	pct_of_organic_transactions_30d,
	impressions_30d,
	clicks_30d,
	avg_position_30d,
	main_keyword,
	main_top_url,
	main_impressions,
	main_clicks,
	main_avg_position,
	best_keyword,
	best_top_url,
	best_impressions,
	best_clicks,
	best_avg_position
	-- ecommerce_conversion_rate_30d,
	-- med_transaction_conversion_rate_30d,
	-- goal_completions_all_goals_30d,
	-- pct_of_organic_goal_completions_all_goals_30d,
	-- goal_conversion_rate_all_goals_30d,
	-- med_goal_conversion_rate_30d,
	-- bounce_rate_30d,
	-- avg_seconds_on_site_30d,
	-- sessions_mom,
	-- sessions_mom_pct,
	-- transaction_revenue_mom,
	-- transaction_revenue_mom_pct,
	-- transactions_mom,
	-- transactions_mom_pct,
	-- goal_completions_all_goals_mom,
	-- goal_completions_all_goals_mom_pct,
	-- sessions_yoy,
	-- sessions_yoy_pct,
	-- transaction_revenue_yoy,
	-- transaction_revenue_yoy_pct,
	-- transactions_yoy,
	-- transactions_yoy_pct,
	-- goal_completions_all_goals_yoy,
	-- goal_completions_all_goals_yoy_pct,
	-- sessions_ttm,
	-- transaction_revenue_ttm,
	-- transactions_ttm,
	-- ecommerce_conversion_rate_ttm,
	-- goal_completions_all_goals_ttm,
	-- goal_conversion_rate_all_goals_ttm,
	-- bounce_rate_ttm,
	-- avg_seconds_on_site_ttm,
	-- gaining_traffic_mom,
	-- gaining_traffic_yoy,
	-- impressions_30d,
	-- clicks_30d,
	-- ctr_30d,
	-- avg_position_30d,
	-- impressions_mom,
	-- impressions_mom_pct,
	-- clicks_mom,
	-- clicks_mom_pct,
	-- ctr_mom,
	-- ctr_mom_pct,
	-- avg_position_mom,
	-- impressions_yoy,
	-- impressions_yoy_pct,
	-- clicks_yoy,
	-- ctr_yoy,	
	-- ctr_yoy_pct,
	-- avg_position_yoy,	
	-- impressions_ttm,
	-- clicks_ttm,
	-- ctr_ttm,
	-- avg_position_ttm,
	-- top_3_keywords,
	-- top_5_keywords,
	-- top_10_keywords,
	-- top_20_keywords,
	-- main_keyword,
	-- main_impressions,
	-- main_clicks,
	-- main_avg_position,
	-- main_top_url,
	-- main_keyword_cannibalization_flag,
	-- main_top_url_clicks,
	-- best_keyword,
	-- best_impressions,
	-- best_clicks,
	-- best_avg_position,
	-- best_top_url,
	-- best_top_url_clicks,
	-- best_keyword_cannibalization_flag
	FROM {{ ref('actions_hierarchy') }}
	WHERE found_at is not null
	WINDOW w1 as (PARTITION BY site, url ORDER BY date asc)

)