
SELECT *
FROM (

  SELECT
  site,
  domain,
  crawl_id,
  eventid,
  urls_to_canonical,
  url_canonical_trailing_slash_match,
  max_trailing_slash_match,
  url,
  url_stripped,
  non_html_url,
  domain_canonical,
  canonical_url,
  canonical_url_stripped,
  query_string_url_first_param,
  query_string_url,
  query_string_canonical_url,  
  url_protocol,
  canonical_url_protocol,
  is_canonicalized,
  crawl_datetime,
  max(crawl_datetime) OVER w3 as latest_crawl_datetime,  
  first_value(query_string_url) over w4 as latest_query_string_url,
  crawl_date,
  crawl_month,
  crawl_report_month,
  found_at_sitemap,
  is_in_sitemap,
  sitemap_canonicalization_score,
  max_sitemap_canonicalization_score,
  http_status_code,
  level,
  schema_type,
  header_content_type,
  word_count, 
  page_title,
  page_title_length,
  description,
  description_length,
  indexable,
  robots_noindex,
  meta_noindex,
  is_self_canonical,
  backlink_count,
  backlink_domain_count,
  redirected_to_url,
  self_redirect,
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
  qt_dec_price,
  qt_cur_price,
  qt_add_to_cart,
  qt_google_maps,
  qt_learn_more,
  qt_reviews,
  qt_size,
  qt_form_submit,
  qt_infinite_scroll,
  decimal_price,
  currency_price,
  add_to_cart,
  learn_more,
  review,
  size,
  paginated_page
  FROM 
  (
    SELECT 
    eventid,
    b.site,
    a.domain,
    crawl_id,
    count(distinct(url)) over w2 urls_to_canonical, 
    url_canonical_trailing_slash_match,
    max(url_canonical_trailing_slash_match) over w2 as max_trailing_slash_match,
    url,  
    url_stripped,
    non_html_url,
    domain_canonical,
    canonical_url,
    canonical_url_stripped,
    query_string_url_first_param,
    query_string_url,
    query_string_canonical_url,  
    url_protocol,
    canonical_url_protocol,
    is_canonicalized,                 
    crawl_datetime,
    crawl_date,
    crawl_month,
    crawl_report_month,
    STRING_AGG (found_at_sitemap) over w1 as found_at_sitemap,
    is_in_sitemap,
    sitemap_canonicalization_score,
    max(sitemap_canonicalization_score) over w1 as max_sitemap_canonicalization_score,
    http_status_code,
    level,
    schema_type,
    header_content_type,
    word_count, 
    page_title,
    page_title_length,
    description,
    description_length,
    indexable,
    robots_noindex,
    meta_noindex,
    is_self_canonical,
    backlink_count,
    backlink_domain_count,
    redirected_to_url,
    self_redirect,
    max(found_at_url) over w1 as found_at_url,
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
    qt_dec_price,
    qt_cur_price,
    qt_add_to_cart,
    qt_google_maps,
    qt_learn_more,
    qt_reviews,
    qt_size,
    qt_form_submit,
    qt_infinite_scroll,
    decimal_price,
    currency_price,
    add_to_cart,
    learn_more,
    review,
    size,
    paginated_page
    FROM
    ( 

      SELECT
      eventid,
      domain,
      crawl_id,
      CASE WHEN url = canonical_url THEN url
          WHEN url_stripped = canonical_url_stripped AND query_string_url_first_param = query_string_canonical_url THEN canonical_url
          ELSE url_stripped
          END as url,
      url_stripped,
      non_html_url,
      domain_canonical,
      canonical_url,
      canonical_url_stripped,
      query_string_url_first_param,
      query_string_url,
      query_string_canonical_url,  
      url_protocol,
      canonical_url_protocol,
      is_canonicalized,  
      is_canonicalized + is_in_sitemap as sitemap_canonicalization_score,
      CASE WHEN substr(url_stripped,length(url),1) = substr(canonical_url_stripped,length(canonical_url),1) THEN 1 ELSE 0 END as url_canonical_trailing_slash_match,            
      crawl_datetime,
      crawl_date,
      crawl_month,
      crawl_report_month,
      found_at_sitemap,
      is_in_sitemap,
      http_status_code,
      level,
      schema_type,
      header_content_type,
      word_count, 
      page_title,
      page_title_length,
      description,
      description_length,
      indexable,
      robots_noindex,
      meta_noindex,
      is_self_canonical,
      backlink_count,
      backlink_domain_count,
      redirected_to_url,
      self_redirect,
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
      qt_dec_price,
      qt_cur_price,
      qt_add_to_cart,
      qt_google_maps,
      qt_learn_more,
      qt_reviews,
      qt_size,
      qt_form_submit,
      qt_infinite_scroll,
      decimal_price,
      currency_price,
      add_to_cart,
      learn_more,
      review,
      size,
      paginated_page 
      FROM 
      (   

        SELECT 
        eventid,
        crawl_id,
        lower(replace(replace(replace(url,'www.',''),'http://',''),'https://','')) as url,
        lower(regexp_replace(replace(replace(replace(url,'www.',''),'http://',''),'https://',''),r'\?.*$','')) as url_stripped,
        regexp_contains(url, '.img$|.png$|.jpg$|.css$.|js$|.pdf$') as non_html_url,
        domain,
        regexp_extract(canonical_url,r'^(?:https?:\/\/)?(?:www\.)?([^\/]+)') as domain_canonical,
        lower(replace(replace(replace(canonical_url,'www.',''),'http://',''),'https://','')) canonical_url,
        lower(regexp_replace(replace(replace(replace(canonical_url,'www.',''),'http://',''),'https://',''),r'\?.*$','')) as canonical_url_stripped,    
        ifnull(split(split(url, '?')[SAFE_ORDINAL(2)],'&')[SAFE_ORDINAL(1)], '') query_string_url_first_param,
        ifnull(split(url, '?')[SAFE_ORDINAL(2)], '') as query_string_url,
        ifnull(split(canonical_url, '?')[SAFE_ORDINAL(2)], '') as query_string_canonical_url,
        case when url like 'https%' then 'https' 
          when url like 'http%' then 'http'
          else 'none' end as url_protocol,
        case when canonical_url like 'https%' then 'https' 
          when canonical_url like 'http%' then 'http'
          else 'none' end as canonical_url_protocol, 
        case 
          when canonical_url is not null then 1
          else 0 end as is_canonicalized,
        crawl_datetime,  
        crawl_date,
        crawl_month,
        crawl_report_month,
        found_at_sitemap,
        case 
            when found_at_sitemap is not null then 1
            else 0 end as is_in_sitemap,
        http_status_code,
        level,
        lower(schema_type) schema_type,
        header_content_type,
        word_count, 
        page_title,
        page_title_length,
        description,
        description_length,
        indexable,
        robots_noindex,
        meta_noindex,
        is_self_canonical,
        cast(backlink_count as int64) backlink_count,
        cast(backlink_domain_count as int64) backlink_domain_count,
        redirected_to_url,
        CASE WHEN regexp_extract(trim(redirected_to_url, '/'),r'^(?:https?:\/\/)?(?:www\.)?([^\/]+)') = regexp_extract(url ,r'^(?:https?:\/\/)?(?:www\.)?([^\/]+)')
          OR regexp_extract(trim(url, '/'),r'^(?:https?:\/\/)?(?:www\.)?([^\/]+)') = regexp_extract(redirected_to_url ,r'^(?:https?:\/\/)?(?:www\.)?([^\/]+)')
          THEN 1 ELSE 0 END as self_redirect,
        found_at_url,
        rel_next_url,
        rel_prev_url,
        links_in_count,
        links_out_count,
        external_links_count,
        internal_links_count,
        lower(replace(replace(replace(h1_tag, "[\"",""),"\"]",""),'"','')) h1_tag,
        lower(replace(replace(replace(h2_tag, "[\"",""),"\"]",""),'"','')) h2_tag,
        redirect_chain,
        redirected_to_status_code,
        is_redirect_loop,
        duplicate_page,
        duplicate_page_count,
        duplicate_body,
        duplicate_body_count,
        form_submit,
        infinite_scroll,
        (LENGTH(decimal_price) - LENGTH(REGEXP_REPLACE(decimal_price, '"', '')))/2 as qt_dec_price,
        (LENGTH(currency_price) - LENGTH(REGEXP_REPLACE(currency_price, '"', '')))/2 as qt_cur_price,
        (LENGTH(add_to_cart) - LENGTH(REGEXP_REPLACE(add_to_cart, '"', '')))/2 as qt_add_to_cart,
        (LENGTH(Google_Maps) - LENGTH(REGEXP_REPLACE(Google_Maps, '"', '')))/2 as qt_google_maps,
        (LENGTH(Learn_More) - LENGTH(REGEXP_REPLACE(Learn_More, '"', '')))/2 as qt_learn_more,
        (LENGTH(Review) - LENGTH(REGEXP_REPLACE(Review, '"', '')))/2 as qt_reviews,
        (LENGTH(Size) - LENGTH(REGEXP_REPLACE(Size, '"', '')))/2 as qt_size,
        (LENGTH(form_submit) - LENGTH(REGEXP_REPLACE(form_submit, '"', '')))/2 as qt_form_submit,
        (LENGTH(infinite_scroll) - LENGTH(REGEXP_REPLACE(infinite_scroll, '"', '')))/2 as qt_infinite_scroll,
        decimal_price,
        currency_price,
        google_maps,
        add_to_cart,
        learn_more,
        review,
        size,
        paginated_page      
        FROM  {{ ref('deepcrawl_latest_crawl_proc')}} 
        WHERE url not like '%target=_blank%'
        AND ( url = primary_url OR primary_url is null )
        AND http_status_code not in (401, 403, 503)
        AND header_content_type = 'text/html'
        
          
      )
    )  a
    LEFT JOIN {{ ref('domains_proc') }} b
    ON (
      a.domain = b.domain
    )
    
    WHERE non_html_url = false
    WINDOW w1 as (PARTITION BY a.domain, crawl_report_month, url ),
    w2 as (PARTITION BY a.domain, crawl_id, canonical_url)
  )
  WHERE max_trailing_slash_match = url_canonical_trailing_slash_match
  AND sitemap_canonicalization_score = max_sitemap_canonicalization_score
  AND self_redirect = 0
  WINDOW w3 as (PARTITION BY domain, crawl_report_month, url),
  w4 as (PARTITION BY domain, crawl_report_month, url ORDER BY found_at_sitemap desc, is_canonicalized desc, crawl_datetime desc, eventid desc )

) 

WHERE latest_crawl_datetime = crawl_datetime
AND latest_query_string_url = query_string_url
GROUP BY   
site,
domain,
crawl_id,
eventid,
urls_to_canonical,
latest_crawl_datetime,  
latest_query_string_url,  
site,
url,
url_stripped,
non_html_url,
domain_canonical,
canonical_url,
canonical_url_stripped,
query_string_url_first_param,
query_string_url,
query_string_canonical_url,
url_protocol,
canonical_url_protocol,
is_canonicalized,
crawl_datetime,
crawl_date,
crawl_month,
crawl_report_month,
found_at_sitemap,
is_in_sitemap,
sitemap_canonicalization_score,
max_sitemap_canonicalization_score,
http_status_code,
level,
schema_type,
header_content_type,
word_count, 
page_title,
page_title_length,
description,
description_length,
indexable,
robots_noindex,
meta_noindex,
is_self_canonical,
backlink_count,
backlink_domain_count,
redirected_to_url,
self_redirect,
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
qt_dec_price,
qt_cur_price,
qt_add_to_cart,
qt_google_maps,
qt_learn_more,
qt_reviews,
qt_size,
qt_form_submit,
qt_infinite_scroll,
decimal_price,
currency_price,
add_to_cart,
learn_more,
review,
size,
paginated_page,
url_canonical_trailing_slash_match,
max_trailing_slash_match