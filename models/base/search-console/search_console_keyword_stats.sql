with gsc as (

	SELECT 
	a.date, 
	unix_date, 
	b.unix_run_date,
	b.unix_mom_date,
	b.unix_yoy_date,	
	a.account,
	a.site,
	a.domain,
	url,
	keyword,
	sum(impressions) impressions,
	sum(clicks) clicks,
	CASE WHEN sum(impressions) > 0 THEN sum(clicks) / sum(impressions) END as ctr,
	CASE WHEN sum(impressions) > 0 THEN sum(avg_position*impressions)/sum(impressions) ELSE null END as avg_position
	FROM {{ ref('search_console_keyword_proc') }} a 
	LEFT JOIN {{ ref('dates') }} b
		ON (
			a.site = b.site AND
			( a.unix_date = b.unix_run_date OR a.unix_date = b.unix_mom_date OR a.unix_date = b.unix_yoy_date )
		)
	WHERE branded_flag = 0 
	GROUP BY a.date, unix_date, unix_run_date, unix_mom_date, unix_yoy_date, account, a.site, a.domain, url, keyword
)

SELECT
date,
account,
site,
domain,
url,
keyword,
CASE WHEN clicks_mom_pct > 0 THEN 'Rising' 
	WHEN clicks_mom_pct < 0 THEN 'Falling'
	WHEN clicks_mom_pct = 0 THEN 'Flat'
	WHEN clicks_mom_pct is null THEN 'New this Month'
	ELSE null END as keyword_bucket,
impressions_30d,
clicks_30d,
sum(clicks_30d) OVER (PARTITION BY date, site) as total_clicks_30d,
ctr_30d,
avg_position_30d,
impressions_mom,
clicks_mom,
clicks_mom_pct,
ctr_mom,
avg_position_mom,
impressions_yoy,
clicks_yoy,
clicks_yoy_pct,
ctr_yoy,
avg_position_yoy
FROM (

	SELECT  
	date,
	account,
	site,
	domain,
	url,
	keyword,
	sum(impressions_30d) as impressions_30d,
	sum(clicks_30d) clicks_30d,
	max(ctr_30d) as ctr_30d,
	max(avg_position_30d) as avg_position_30d,
	sum(impressions_mom) as impressions_mom,
	sum(clicks_mom) clicks_mom,
	CASE WHEN sum(clicks_mom) > 0 THEN (sum(clicks_30d) - sum(clicks_mom)) / sum(clicks_mom) ELSE null END as clicks_mom_pct,
	max(ctr_mom) as ctr_mom,
	min(avg_position_mom) as avg_position_mom,
	sum(impressions_yoy) as impressions_yoy,
	sum(clicks_yoy) clicks_yoy,
	CASE WHEN sum(clicks_yoy) > 0 THEN (sum(clicks_30d) - sum(clicks_yoy)) / sum(clicks_yoy) ELSE null END as clicks_yoy_pct,
	max(ctr_yoy) as ctr_yoy,
	min(avg_position_yoy) as avg_position_yoy
	FROM (

		SELECT
		date_from_unix_date(unix_run_date) date,
		account,
		site,
		domain,
		url,
		keyword,
		case when unix_date = unix_run_date then impressions else 0 end as impressions_30d,
		case when unix_date = unix_run_date then clicks else 0 end as clicks_30d,
		case when unix_date = unix_run_date then ctr else 0 end as ctr_30d,
		case when unix_date = unix_run_date then avg_position else 0 end as avg_position_30d,
		null as impressions_mom,
		null as clicks_mom,
		null as ctr_mom,
		null as avg_position_mom,
		null as impressions_yoy,
		null as clicks_yoy,
		null as ctr_yoy,
		null as avg_position_yoy
		FROM gsc

		UNION ALL	

		SELECT
		date_from_unix_date(unix_run_date) date,
		account,
		site,
		domain,
		url,
		keyword,
		null as impressions_30d,
		null as clicks_30d,
		null as ctr_30d,
		null as avg_position_30d,
		case when unix_date = unix_mom_date then impressions else 0 end as impressions_mom,
		case when unix_date = unix_mom_date then clicks else 0 end as clicks_mom,
		case when unix_date = unix_mom_date then ctr else 0 end as ctr_mom,
		case when unix_date = unix_mom_date then avg_position else 0 end as avg_position_mom,
		null as impressions_yoy,
		null as clicks_yoy,
		null as ctr_yoy,
		null as avg_position_yoy
		FROM gsc

		UNION ALL

		SELECT
		date_from_unix_date(unix_run_date) date,
		account,
		site,
		domain,
		url,
		keyword,
		null as impressions_30d,
		null as clicks_30d,
		null as ctr_30d,
		null as avg_position_30d,
		null as impressions_mom,
		null as clicks_mom,
		null as ctr_mom,
		null as avg_position_mom,
		case when unix_date = unix_yoy_date then impressions else 0 end as impressions_yoy,
		case when unix_date = unix_yoy_date then clicks else 0 end as clicks_yoy,
		case when unix_date = unix_yoy_date then ctr else 0 end as ctr_yoy,
		case when unix_date = unix_yoy_date then avg_position else 0 end as avg_position_yoy
		FROM gsc
	)
	group by date, account, site, domain, url, keyword
)