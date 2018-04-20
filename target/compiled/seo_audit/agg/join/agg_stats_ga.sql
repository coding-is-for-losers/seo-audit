with ga as (

	select 
		date, client, url, 
		users_90d, sessions_90d, leads_90d, transactions_90d,
		null as ref_domain_count, null as avg_trust_flow, null as avg_citation_flow, null as gsc_keyword_count,
		null as gsc_impressions, null as gsc_clicks, null as semrush_keyword_count, null as semrush_total_cpc, 
		null as semrush_search_volume

	from `curious-domain-121318`.`seo_audit`.`ga_stats`

),

stats as (

	select 
		date, client, url,
		null as users_90d, null as sessions_90d, null as leads_90d, null as transactions_90d,
		ref_domain_count, avg_trust_flow, avg_citation_flow, gsc_keyword_count,
		gsc_impressions, gsc_clicks, semrush_keyword_count, semrush_total_cpc, semrush_search_volume
	from `curious-domain-121318`.`seo_audit`.`agg_stats_client`

)


SELECT
    date, 
    url,
    client,
    sum(users_90d) users_90d,
	sum(sessions_90d) sessions_90d,
	sum(leads_90d) leads_90d,
	sum(transactions_90d) transactions_90d,
    sum(ref_domain_count) ref_domain_count,
	sum(avg_trust_flow) avg_trust_flow,
	sum(avg_citation_flow) avg_citation_flow,
	sum(gsc_keyword_count) gsc_keyword_count,
	sum(gsc_impressions) gsc_impressions,
	sum(gsc_clicks) gsc_clicks,
	sum(semrush_keyword_count) semrush_keyword_count,
	sum(semrush_total_cpc) semrush_total_cpc,
	sum(semrush_search_volume) semrush_search_volume
FROM
(
    SELECT *
    FROM 
      ga
    UNION ALL
    SELECT *
    FROM 
      stats
) 
GROUP BY
    date, url, client