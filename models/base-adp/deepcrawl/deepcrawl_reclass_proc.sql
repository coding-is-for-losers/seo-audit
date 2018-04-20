SELECT 
client,
platform,
domain,
domain_canonical,
a.url url,
url_stripped,
canonical_url,
canonical_url_stripped,
canonical_status,
path_count,
last_path,
a.last_subfolder last_subfolder,
'' as last_subfolder_http_status,
last_subfolder_canonical,
urls_to_canonical,
crawl_datetime,
found_at_sitemap,
a.http_status_code http_status_code,
level,
schema_type,
header_content_type,
word_count, 
med_word_count,
page_title,
page_title_length,
description,
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
a.first_path first_path,
a.first_subfolder first_subfolder,
'' as first_subfolder_http_status,
a.second_subfolder second_subfolder,
'' as second_subfolder_http_status,
a.query_string query_string,
a.filename filename,
a.classification original_class,
-- b.query_string query_string_rule,
-- e.pct_of_classification query_string_pct_of_class,
-- e.pct_of_value query_string_pct_of_value,
-- h.classification class_if_unclassified_query_string,
-- c.filename filename_rule,
-- f.pct_of_classification filename_pct_of_class,
-- f.pct_of_value filename_pct_of_value,
-- i.classification class_if_unclassified_filename,
-- d.first_path first_path_rule,
-- g.pct_of_classification first_path_pct_of_class,
-- g.pct_of_value first_path_pct_of_value,
-- j.classification class_if_unclassified_first_path,
-- coalesce(h.classification, i.classification, j.classification) class_if_unclassified,
-- case when ( b.query_string is not null and ( a.query_string != b.query_string ) and e.pct_of_value < {{ var('deepcrawl_pct_of_value_rule')}} ) then 1 else 0 end as reclass_query_string,
-- case when ( c.filename is not null and ( a.filename != c.filename ) and ( f.pct_of_value < {{ var('deepcrawl_pct_of_value_rule')}} ) ) then 1 else 0 end as reclass_filename, 
-- case when ( d.first_path is not null and (a.first_path != d.first_path ) and ( g.pct_of_value < {{ var('deepcrawl_pct_of_value_rule')}} ) ) then 1 else 0 end as reclass_first_path,
class_sitemap,
class_schema,
class_google_maps,
flag_blog_path,
flag_blog_h1,
flag_high_word_count,
flag_thin_page,
flag_reviews,
flag_select_size,
flag_add_to_cart,
flag_prices,
flag_above_avg_prices,
flag_form_submit,
flag_learn_more,
flag_info_path,
flag_paginated,
product_score,
category_score,
article_score
FROM {{ ref('deepcrawl_class') }} a
-- LEFT JOIN {{ ref('deepcrawl_rules_query_string') }} b
-- ON (  a.classification = b.classification )
-- LEFT JOIN {{ ref('deepcrawl_rules_filename') }} c
-- ON (  a.classification = c.classification )
-- LEFT JOIN {{ ref('deepcrawl_rules_first_path') }} d
-- ON (  a.classification = d.classification )
-- LEFT JOIN {{ ref('deepcrawl_class_stats_query_string') }} e
-- ON (  a.classification = e.classification AND 
-- 	a.query_string = e.query_string ) 
-- LEFT JOIN {{ ref('deepcrawl_class_stats_filename') }} f
-- ON (  a.classification = f.classification AND 
-- 	a.filename = f.filename ) 
-- LEFT JOIN {{ ref('deepcrawl_class_stats_first_path') }} g
-- ON (  a.classification = g.classification AND 
-- 	a.first_path = g.first_path ) 
-- LEFT JOIN {{ ref('deepcrawl_rules_query_string_unclassified') }} h
-- ON (  a.query_string = h.query_string )
-- LEFT JOIN {{ ref('deepcrawl_rules_filename_unclassified') }} i
-- ON (  a.filename = i.filename )
-- LEFT JOIN {{ ref('deepcrawl_rules_first_path_unclassified') }} j
-- ON (  a.first_path = j.first_path )
-- LEFT JOIN {{ ref('deepcrawl_urls') }} k
-- ON (  a.first_subfolder = k.url )
-- LEFT JOIN {{ ref('deepcrawl_urls') }} l
-- ON (  a.second_subfolder = l.url )
-- LEFT JOIN {{ ref('deepcrawl_urls') }} m
-- ON (  a.last_subfolder = m.url )