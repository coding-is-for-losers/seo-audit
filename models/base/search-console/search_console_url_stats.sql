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
	impressions,
	clicks,
	ctr,
	avg_position
	FROM {{ ref('search_console_url_proc') }} a
	LEFT JOIN {{ ref('dates') }} b
	ON (
		a.site = b.site AND
		( a.unix_date = b.unix_run_date OR a.unix_date = b.unix_mom_date OR a.unix_date = b.unix_yoy_date )
	)
)

SELECT  
date,
account,
site,
domain,
url,
sum(impressions_30d) as impressions_30d,
sum(clicks_30d) clicks_30d,
max(ctr_30d) as ctr_30d,
max(avg_position_30d) as avg_position_30d,
sum(impressions_mom) as impressions_mom,
sum(clicks_mom) clicks_mom,
max(ctr_mom) as ctr_mom,
min(avg_position_mom) as avg_position_mom,
sum(impressions_yoy) as impressions_yoy,
sum(clicks_yoy) clicks_yoy,
max(ctr_yoy) as ctr_yoy,
min(avg_position_yoy) as avg_position_yoy
FROM (

	SELECT
	date_from_unix_date(unix_run_date) date,
	account,
	site,
	domain,
	url,
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
group by date, account, site, domain, url