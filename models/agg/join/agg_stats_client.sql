SELECT 
a.date date, 
case when c.canonical_url like '%?%' then a.url else trim(regexp_replace(a.url,r'\?.*$',''),'/') end as url,
b.client client,
max(gsc_top_keyword_90d) as gsc_top_keyword_90d,
max(gsc_top_url_for_keyword_90d) as gsc_top_url_for_keyword_90d,
max(semrush_top_keyword_vol) as semrush_top_keyword_vol,
max(semrush_top_keyword_cpc) as semrush_top_keyword_cpc,
max(gsc_top_url_impressions_for_keyword_90d) as gsc_top_url_impressions_for_keyword_90d,
max(ref_domain_count) as ref_domain_count,
max(avg_trust_flow) as avg_trust_flow,
max(avg_citation_flow) as avg_citation_flow,
max(gsc_keyword_count_90d) as gsc_keyword_count_90d,
max(gsc_impressions_90d) as gsc_impressions_90d,
max(gsc_clicks_90d) as gsc_clicks_90d,	
max(gsc_ctr_90d) as gsc_ctr_90d,
max(gsc_top_keyword_impressions_90d) as gsc_top_keyword_impressions_90d, 
max(gsc_top_keyword_clicks_90d) as gsc_top_keyword_clicks_90d, 
max(gsc_top_keyword_ctr_90d) as gsc_top_keyword_ctr_90d,
max(semrush_keyword_count) semrush_keyword_count,
max(semrush_total_cpc) semrush_total_cpc,
max(semrush_total_search_volume) semrush_total_search_volume,
max(semrush_min_position) semrush_min_position,
max(semrush_top_keyword_vol_vol) semrush_top_keyword_vol_vol, 
max(semrush_top_keyword_vol_cpc) semrush_top_keyword_vol_cpc, 
max(semrush_top_keyword_vol_pos) semrush_top_keyword_vol_pos, 
max(semrush_top_keyword_cpc_vol) semrush_top_keyword_cpc_vol, 
max(semrush_top_keyword_cpc_cpc) semrush_top_keyword_cpc_cpc,
max(semrush_top_keyword_cpc_pos) semrush_top_keyword_cpc_pos,
max(sessions_30d) sessions_30d,
max(revenue_30d) revenue_30d,
max(transactions_30d) transactions_30d,
max(pageviews_30d) pageviews_30d,
max(sessions_mom) sessions_mom,
max(revenue_mom) revenue_mom,
max(transactions_mom) transactions_mom,
max(pageviews_mom) pageviews_mom,
max(sessions_yoy) sessions_yoy,
max(revenue_yoy) revenue_yoy,
max(transactions_yoy) transactions_yoy,
max(pageviews_yoy) pageviews_yoy
FROM 
  {{ref('agg_stats')}} a
LEFT JOIN {{ref('accounts_proc')}} b
ON ( a.account = b.account
	and a.platform = b.platform )
LEFT JOIN {{ ref('deepcrawl_class') }} c
ON (
	a.url = c.url
	)
GROUP BY date, client, url