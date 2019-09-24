SELECT 
date, 
crawl_date,
coalesce(a.site, b.site) site,
coalesce(a.domain, b.domain) domain,
coalesce(a.url, b.url) url,
-- deepcrawl data
case when found_at_sitemap is not null then 'yes' else 'no' end as in_sitemap,
found_at_sitemap,
page_type,
rank() over w5 as page_type_rank,
url_stripped,
url_protocol,
canonical_url_protocol,
protocol_match,
protocol_count,
canonical_url,
canonical_url_stripped,
canonical_status,
urls_to_canonical,
first_subfolder,
sum(sessions_30d) OVER w2 first_subfolder_sessions_30d,
second_subfolder,
sum(sessions_30d) OVER w3 second_subfolder_sessions_30d,
last_subfolder,
sum(sessions_30d) OVER w4 last_subfolder_sessions_30d,
last_subfolder_canonical,
http_status_code,
level,
schema_type,
header_content_type,
word_count, 
page_title,
case when regexp_contains(page_title, main_keyword) OR regexp_contains(page_title, best_keyword)  then 1
	when ( main_keyword is not null or main_keyword != '' ) then 0 else 2 end as title_contains_top_keyword,
page_title_length,
description,
case when regexp_contains(description, main_keyword) OR regexp_contains(description, best_keyword) then 1 
	when ( main_keyword is not null or main_keyword != '' ) then 0 else 2 end as description_contains_top_keyword,
description_length,
indexable,
robots_noindex,
is_self_canonical,
redirected_to_url,
found_at_url,
rel_next_url,
rel_prev_url,
links_in_count internal_links_in_count,
PERCENTILE_DISC(links_in_count, 0.25 IGNORE NULLS) OVER w1 AS bottom_quartile_internal_links_in_count,
PERCENTILE_DISC(links_in_count, 0.75 IGNORE NULLS) OVER w1 AS top_quartile_internal_links_in_count,
links_out_count,
external_links_count,
internal_links_count internal_links_out_count,
PERCENTILE_DISC(internal_links_count, 0.25 IGNORE NULLS) OVER w1 AS bottom_quartile_internal_links_out_count,
flag_paginated,
case when regexp_contains(coalesce(a.url, b.url), r'\/\d') then 1 else 0 end as url_contains_digit,
h1_tag,
h2_tag,
redirect_chain,
redirected_to_status_code,
is_redirect_loop,
duplicate_page,
duplicate_page_count,
duplicate_body,
duplicate_body_count,
-- gsc + ga data
sessions_30d,
PERCENTILE_DISC(sessions_30d, 0.25 IGNORE NULLS) OVER w1 AS bottom_quartile_sessions_30d,
PERCENTILE_DISC(sessions_30d, 0.5 IGNORE NULLS) OVER w1 AS med_sessions_30d,
PERCENTILE_DISC(sessions_30d, 0.75 IGNORE NULLS) OVER w1 AS top_quartile_sessions_30d,
total_organic_sessions_30d,
CASE WHEN total_organic_sessions_30d > 0 THEN sessions_30d / total_organic_sessions_30d ELSE null END as pct_of_organic_sessions_30d,
transaction_revenue_30d,
transactions_30d,
CASE WHEN total_organic_transactions_30d > 0 THEN transactions_30d / total_organic_transactions_30d ELSE null END as pct_of_organic_transactions_30d,
ecommerce_conversion_rate_30d,
PERCENTILE_DISC(ecommerce_conversion_rate_30d, 0.5 IGNORE NULLS) OVER w1 AS med_transaction_conversion_rate_30d,
goal_completions_all_goals_30d,
CASE WHEN total_organic_goal_completions_all_goals_30d > 0 THEN goal_completions_all_goals_30d / total_organic_goal_completions_all_goals_30d ELSE null END as pct_of_organic_goal_completions_all_goals_30d,
goal_conversion_rate_all_goals_30d,
PERCENTILE_DISC(goal_conversion_rate_all_goals_30d, 0.5 IGNORE NULLS) OVER w1 AS med_goal_conversion_rate_30d,
blended_conversions_30d,
PERCENTILE_DISC(blended_conversions_30d, 0.25 IGNORE NULLS) OVER w1 AS bottom_quartile_blended_conversions_30d,
PERCENTILE_DISC(blended_conversions_30d, 0.5 IGNORE NULLS) OVER w1 AS med_blended_conversions_30d,
PERCENTILE_DISC(blended_conversions_30d, 0.75 IGNORE NULLS) OVER w1 AS top_quartile_blended_conversions_30d,
blended_conversion_rate_30d,
PERCENTILE_DISC(blended_conversion_rate_30d, 0.25 IGNORE NULLS) OVER w1 AS bottom_quartile_blended_conversion_rate_30d,
PERCENTILE_DISC(blended_conversion_rate_30d, 0.5 IGNORE NULLS) OVER w1 AS med_blended_conversion_rate_30d,
PERCENTILE_DISC(blended_conversion_rate_30d, 0.75 IGNORE NULLS) OVER w1 AS top_quartile_blended_conversion_rate_30d,
bounce_rate_30d,
avg_seconds_on_site_30d,
sessions_mom,
case when sessions_mom > 0 then (sessions_30d-sessions_mom)/sessions_mom else null end sessions_mom_pct,
case when total_organic_sessions_mom > 0 then (total_organic_sessions_30d-total_organic_sessions_mom)/total_organic_sessions_mom else null end total_organic_sessions_mom_pct,
transaction_revenue_mom,
case when transaction_revenue_mom > 0 then (transaction_revenue_30d-transaction_revenue_mom)/transaction_revenue_mom else null end transaction_revenue_mom_pct,
transactions_mom,
case when transactions_mom > 0 then (transactions_30d-transactions_mom)/transactions_mom else null end transactions_mom_pct,
ecommerce_conversion_rate_mom,
case when ecommerce_conversion_rate_mom > 0 then (ecommerce_conversion_rate_30d-ecommerce_conversion_rate_mom)/ecommerce_conversion_rate_mom else null end ecommerce_conversion_rate_mom_pct,
case when total_organic_ecommerce_conversion_rate_mom > 0 then (total_organic_ecommerce_conversion_rate_30d-total_organic_ecommerce_conversion_rate_mom)/total_organic_ecommerce_conversion_rate_mom else null end total_organic_ecommerce_conversion_rate_mom_pct,
goal_completions_all_goals_mom,
case when transactions_mom > 0 then (goal_completions_all_goals_30d-goal_completions_all_goals_mom)/goal_completions_all_goals_mom else null end goal_completions_all_goals_mom_pct,
goal_conversion_rate_all_goals_mom,
case when goal_conversion_rate_all_goals_mom > 0 then (goal_conversion_rate_all_goals_30d-goal_conversion_rate_all_goals_mom)/goal_conversion_rate_all_goals_mom else null end goal_conversion_rate_all_goals_mom_pct,
case when total_organic_goal_conversion_rate_mom > 0 then (total_organic_goal_conversion_rate_30d-total_organic_goal_conversion_rate_mom)/total_organic_goal_conversion_rate_mom else null end total_organic_goal_conversion_rate_mom_pct,
sessions_yoy,
case when sessions_yoy > 0 then (sessions_30d-sessions_yoy)/sessions_yoy else null end sessions_yoy_pct,
case when total_organic_sessions_yoy > 0 then (total_organic_sessions_30d-total_organic_sessions_yoy)/total_organic_sessions_yoy else null end total_organic_sessions_yoy_pct,
transaction_revenue_yoy,
case when transaction_revenue_yoy > 0 then (transaction_revenue_30d-transaction_revenue_yoy)/transaction_revenue_yoy else null end transaction_revenue_yoy_pct,
transactions_yoy,
case when transactions_yoy > 0 then (transactions_30d-transactions_yoy)/transactions_yoy else null end transactions_yoy_pct,
ecommerce_conversion_rate_yoy,
case when ecommerce_conversion_rate_yoy > 0 then (ecommerce_conversion_rate_30d-ecommerce_conversion_rate_yoy)/ecommerce_conversion_rate_yoy else null end ecommerce_conversion_rate_yoy_pct,
case when total_organic_ecommerce_conversion_rate_yoy > 0 then (total_organic_ecommerce_conversion_rate_30d-total_organic_ecommerce_conversion_rate_yoy)/total_organic_ecommerce_conversion_rate_yoy else null end total_organic_ecommerce_conversion_rate_yoy_pct,
goal_completions_all_goals_yoy,
case when transactions_yoy > 0 then (goal_completions_all_goals_30d-goal_completions_all_goals_yoy)/goal_completions_all_goals_yoy else null end goal_completions_all_goals_yoy_pct,
goal_conversion_rate_all_goals_yoy,
case when goal_conversion_rate_all_goals_yoy > 0 then (goal_conversion_rate_all_goals_30d-goal_conversion_rate_all_goals_yoy)/goal_conversion_rate_all_goals_yoy else null end goal_conversion_rate_all_goals_yoy_pct,
case when total_organic_goal_conversion_rate_yoy > 0 then (total_organic_goal_conversion_rate_30d-total_organic_goal_conversion_rate_yoy)/total_organic_goal_conversion_rate_yoy else null end total_organic_goal_conversion_rate_yoy_pct,
case when sessions_30d > sessions_mom then TRUE else FALSE end as gaining_traffic_mom,
case when sessions_30d > sessions_yoy then TRUE else FALSE end as gaining_traffic_yoy,
backlink_count,
backlink_domain_count ref_domain_count,
PERCENTILE_DISC(backlink_domain_count, 0.5 IGNORE NULLS) OVER w1 AS med_ref_domain_count,
PERCENTILE_DISC(backlink_domain_count, 0.25 IGNORE NULLS) OVER w1 AS bottom_quartile_ref_domain_count,
impressions_30d,
total_impressions_30d,
clicks_30d,
total_clicks_30d,
ctr_30d,
PERCENTILE_DISC(ctr_30d, 0.5 IGNORE NULLS) OVER w1 AS med_ctr_30d,
total_ctr_30d,
avg_position_30d,
impressions_mom,
total_impressions_mom,
case when impressions_mom > 0 then (impressions_30d-impressions_mom)/impressions_mom else null end impressions_mom_pct,
case when total_impressions_mom > 0 then (total_impressions_30d-total_impressions_mom)/total_impressions_mom else null end total_impressions_mom_pct,
clicks_mom,
total_clicks_mom,
case when clicks_mom > 0 then (clicks_30d-clicks_mom)/clicks_mom else null end clicks_mom_pct,
case when total_clicks_mom > 0 then (total_clicks_30d-total_clicks_mom)/total_clicks_mom else null end total_clicks_mom_pct,
ctr_mom,
total_ctr_mom,
case when ctr_mom > 0 then (ctr_30d-ctr_mom)/ctr_mom else null end ctr_mom_pct,
case when total_ctr_mom > 0 then (total_ctr_30d-total_ctr_mom)/total_ctr_mom else null end total_ctr_mom_pct,
avg_position_mom,
impressions_yoy,
total_impressions_yoy,
case when impressions_yoy > 0 then (impressions_30d-impressions_yoy)/impressions_yoy else null end impressions_yoy_pct,
case when total_impressions_yoy > 0 then (total_impressions_30d-total_impressions_yoy)/total_impressions_yoy else null end total_impressions_yoy_pct,
clicks_yoy,
total_clicks_yoy,
case when clicks_yoy > 0 then (clicks_30d-clicks_yoy)/clicks_yoy else null end clicks_yoy_pct,
case when total_clicks_yoy > 0 then (total_clicks_30d-total_clicks_yoy)/total_clicks_yoy else null end total_clicks_yoy_pct,
ctr_yoy,	
total_ctr_yoy,
case when ctr_yoy > 0 then (ctr_30d-ctr_yoy)/ctr_yoy else null end ctr_yoy_pct,
case when total_ctr_yoy > 0 then (total_ctr_30d-total_ctr_yoy)/total_ctr_yoy else null end total_ctr_yoy_pct,
avg_position_yoy,	
top_5_keywords,
top_10_keywords,
top_20_keywords,
main_keyword,
main_impressions,
main_clicks,
main_avg_position,
main_top_url,
CASE WHEN main_top_url != a.url and main_keyword != '' THEN 1 ELSE 0 END as main_keyword_cannibalization_flag,
main_top_url_clicks,
best_keyword,
best_impressions,
best_clicks,
best_avg_position,
best_top_url,
best_top_url_clicks,
CASE WHEN best_top_url != a.url and best_keyword != '' THEN 1 ELSE 0 END as best_keyword_cannibalization_flag
FROM 
  {{ref('agg_traffic')}} a
FULL OUTER JOIN {{ref('deepcrawl_stats')}} b
ON ( a.url = b.url
	and a.site = b.site )
WINDOW w1 as (PARTITION BY b.domain, date),
w2 as (PARTITION BY b.domain, crawl_date, first_subfolder),
w3 as (PARTITION BY b.domain, crawl_date, second_subfolder),
w4 as (PARTITION BY b.domain, crawl_date, last_subfolder),
w5 as (PARTITION BY b.domain, crawl_date, page_type ORDER BY sessions_30d desc)