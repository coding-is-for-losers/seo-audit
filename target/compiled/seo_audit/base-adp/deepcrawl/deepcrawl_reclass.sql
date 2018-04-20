SELECT 
case 
	when original_class != 'unclassified' then original_class
	when class_schema is not null then class_schema
	when class_sitemap is not null then class_sitemap
	-- when original_class = 'unclassified' and class_if_unclassified is not null then class_if_unclassified
	when class_google_maps is not null and class_google_maps != original_class then class_google_maps
	when ( flag_info_path = 1 and flag_blog_path = 0 and flag_blog_h1 = 0 and original_class != 'info' ) then 'info'
	when product_score >=2 and original_class != 'product' then 'product'
	when flag_prices = 1 and original_class != 'product_category' then 'product_category'
	when flag_form_submit = 1 and original_class != 'lead_generation' then 'lead_generation'
	when flag_learn_more = 1  and original_class != 'blog_category' then 'blog_category'
	when class_sitemap is not null and original_class != class_sitemap then class_sitemap
	when article_score >= 1 and original_class != 'article' then 'article'
	else 'unclassified' end as page_type,
client,
platform,
domain,
domain_canonical,
url,
url_stripped,
canonical_url,
canonical_url_stripped,
canonical_status,
urls_to_canonical,
crawl_datetime,
found_at_sitemap,
http_status_code,
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
path_count,
first_path,
last_path,
last_subfolder,
last_subfolder_canonical,
last_subfolder_http_status,
first_subfolder,
first_subfolder_http_status,
second_subfolder,
second_subfolder_http_status,
query_string,
filename,
original_class,
-- query_string_rule,
-- query_string_pct_of_class,
-- query_string_pct_of_value,
-- class_if_unclassified_query_string,
-- filename_rule,
-- filename_pct_of_class,
-- filename_pct_of_value,
-- class_if_unclassified_filename,
-- first_path_rule,
-- first_path_pct_of_class,
-- first_path_pct_of_value,
-- class_if_unclassified_first_path,
-- class_if_unclassified,
-- reclass_query_string,
-- reclass_filename, 
-- reclass_first_path,
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
FROM 
`curious-domain-121318`.`seo_audit`.`deepcrawl_reclass_proc`