SELECT
date,
site,
'Technical' as action_type,
page_type,
crawl_action as action,
crawl_action_diff as status,
crawl_action_priority as priority,
url,
sessions_30d,
pct_of_organic_sessions_30d,
transaction_revenue_30d,
transactions_30d,
pct_of_organic_transactions_30d,
impressions_mom_pct,
ctr_mom_pct,
sessions_mom_pct,
backlink_count_diff,
ref_domain_count_diff,
internal_links_in_count_diff,
internal_links_out_count_diff,
top_3_keywords_diff,
top_10_keywords_diff
FROM {{ ref('actions_diffs') }}
WHERE crawl_action not like '%removed%'
AND crawl_action_diff not in ('None','')
AND crawl_action_diff is not null

UNION ALL

SELECT
date,
site,
'Technical' as action_type,
page_type,
http_status_action as action,
http_status_action_diff as status,
http_status_action_priority as priority,
url,
sessions_30d,
pct_of_organic_sessions_30d,
transaction_revenue_30d,
transactions_30d,
pct_of_organic_transactions_30d,
impressions_mom_pct,
ctr_mom_pct,
sessions_mom_pct,
backlink_count_diff,
ref_domain_count_diff,
internal_links_in_count_diff,
internal_links_out_count_diff,
top_3_keywords_diff,
top_10_keywords_diff
FROM {{ ref('actions_diffs') }}
WHERE http_status_action_diff not in ('None','')
AND http_status_action_diff is not null

UNION ALL

SELECT
date,
site,
'Technical' as action_type,
page_type,
sitemap_action as action,
sitemap_action_diff as status,
sitemap_action_priority as priority,
url,
sessions_30d,
pct_of_organic_sessions_30d,
transaction_revenue_30d,
transactions_30d,
pct_of_organic_transactions_30d,
impressions_mom_pct,
ctr_mom_pct,
sessions_mom_pct,
backlink_count_diff,
ref_domain_count_diff,
internal_links_in_count_diff,
internal_links_out_count_diff,
top_3_keywords_diff,
top_10_keywords_diff
FROM {{ ref('actions_diffs') }}
WHERE sitemap_action_diff not in ('None','')
AND sitemap_action_diff is not null


UNION ALL

SELECT
date,
site,
'Technical' as action_type,
page_type,
canonical_action as action,
canonical_action_diff as status,
canonical_action_priority as priority,
url,
sessions_30d,
pct_of_organic_sessions_30d,
transaction_revenue_30d,
transactions_30d,
pct_of_organic_transactions_30d,
impressions_mom_pct,
ctr_mom_pct,
sessions_mom_pct,
backlink_count_diff,
ref_domain_count_diff,
internal_links_in_count_diff,
internal_links_out_count_diff,
top_3_keywords_diff,
top_10_keywords_diff
FROM {{ ref('actions_diffs') }}
WHERE canonical_action_diff not in ('None','')
AND canonical_action_diff is not null

UNION ALL

SELECT
date,
site,
'On-page' as action_type,
page_type,
content_action as action,
content_action_diff as status,
mktg_action_priority as priority,
url,
sessions_30d,
pct_of_organic_sessions_30d,
transaction_revenue_30d,
transactions_30d,
pct_of_organic_transactions_30d,
impressions_mom_pct,
ctr_mom_pct,
sessions_mom_pct,
backlink_count_diff,
ref_domain_count_diff,
internal_links_in_count_diff,
internal_links_out_count_diff,
top_3_keywords_diff,
top_10_keywords_diff
FROM {{ ref('actions_diffs') }}
WHERE content_action not like 'Rising%' and content_action not like 'Below%'
AND content_action_diff not in ('None','')
AND content_action_diff is not null

UNION ALL

SELECT
date,
site,
'On-page' as action_type,
page_type,
schema_action as action,
schema_action_diff as status,
mktg_action_priority as priority,
url,
sessions_30d,
pct_of_organic_sessions_30d,
transaction_revenue_30d,
transactions_30d,
pct_of_organic_transactions_30d,
impressions_mom_pct,
ctr_mom_pct,
sessions_mom_pct,
backlink_count_diff,
ref_domain_count_diff,
internal_links_in_count_diff,
internal_links_out_count_diff,
top_3_keywords_diff,
top_10_keywords_diff
FROM {{ ref('actions_diffs') }}
WHERE schema_action_diff not in ('None','')
AND schema_action_diff is not null

UNION ALL

SELECT
date,
site,
'On-page' as action_type,
page_type,
meta_rewrite_action as action,
meta_rewrite_action_diff as status,
mktg_action_priority as priority,
url,
sessions_30d,
pct_of_organic_sessions_30d,
transaction_revenue_30d,
transactions_30d,
pct_of_organic_transactions_30d,
impressions_mom_pct,
ctr_mom_pct,
sessions_mom_pct,
backlink_count_diff,
ref_domain_count_diff,
internal_links_in_count_diff,
internal_links_out_count_diff,
top_3_keywords_diff,
top_10_keywords_diff
FROM {{ ref('actions_diffs') }}
WHERE meta_rewrite_action_diff not in ('None','')
AND meta_rewrite_action_diff is not null

UNION ALL

SELECT
date,
site,
'Off-page' as action_type,
page_type,
external_link_action as action,
external_link_action_diff as status,
mktg_action_priority as priority,
url,
sessions_30d,
pct_of_organic_sessions_30d,
transaction_revenue_30d,
transactions_30d,
pct_of_organic_transactions_30d,
impressions_mom_pct,
ctr_mom_pct,
sessions_mom_pct,
backlink_count_diff,
ref_domain_count_diff,
internal_links_in_count_diff,
internal_links_out_count_diff,
top_3_keywords_diff,
top_10_keywords_diff
FROM {{ ref('actions_diffs') }}
WHERE external_link_action_diff not in ('None','')
AND external_link_action_diff is not null


UNION ALL

SELECT
date,
site,
'Architecture' as action_type,
page_type,
cannibalization_action as action,
cannibalization_action_diff as status,
mktg_action_priority as priority,
url,
sessions_30d,
pct_of_organic_sessions_30d,
transaction_revenue_30d,
transactions_30d,
pct_of_organic_transactions_30d,
impressions_mom_pct,
ctr_mom_pct,
sessions_mom_pct,
backlink_count_diff,
ref_domain_count_diff,
internal_links_in_count_diff,
internal_links_out_count_diff,
top_3_keywords_diff,
top_10_keywords_diff
FROM {{ ref('actions_diffs') }}
WHERE cannibalization_action_diff not in ('None','')
AND cannibalization_action_diff is not null


UNION ALL

SELECT
date,
site,
'Architecture' as action_type,
page_type,
internal_link_action as action,
internal_link_action_diff as status,
mktg_action_priority as priority,
url,
sessions_30d,
pct_of_organic_sessions_30d,
transaction_revenue_30d,
transactions_30d,
pct_of_organic_transactions_30d,
impressions_mom_pct,
ctr_mom_pct,
sessions_mom_pct,
backlink_count_diff,
ref_domain_count_diff,
internal_links_in_count_diff,
internal_links_out_count_diff,
top_3_keywords_diff,
top_10_keywords_diff
FROM {{ ref('actions_diffs') }}
WHERE internal_link_action_diff not in ('None','')
AND internal_link_action_diff is not null
