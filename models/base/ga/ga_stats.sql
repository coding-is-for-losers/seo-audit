with ga as (

	SELECT 
	a.date report_date, 
	unix_date, 
	b.unix_run_date,
	b.unix_mom_date,
	b.unix_yoy_date,
	a.account, 
	a.site, 
	a.domain,
	url, 
	sessions,
	transaction_revenue,
	transactions,
	ecommerce_conversion_rate,
	goal_completions_all_goals,
	goal_conversion_rate_all_goals,
	bounce_rate,
	avg_seconds_on_site
	FROM {{ ref('ga_proc') }} a
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
sum(sessions_30d) as sessions_30d,
sum(transaction_revenue_30d) transaction_revenue_30d,
sum(transactions_30d) as transactions_30d,
max(ecommerce_conversion_rate_30d) as ecommerce_conversion_rate_30d,
sum(goal_completions_all_goals_30d) as goal_completions_all_goals_30d,
max(goal_conversion_rate_all_goals_30d) as goal_conversion_rate_all_goals_30d,
max(bounce_rate_30d) bounce_rate_30d,
max(avg_seconds_on_site_30d) as avg_seconds_on_site_30d,
sum(sessions_mom) as sessions_mom,
sum(transaction_revenue_mom) transaction_revenue_mom,
sum(transactions_mom) as transactions_mom,
max(ecommerce_conversion_rate_mom) as ecommerce_conversion_rate_mom,
sum(goal_completions_all_goals_mom) as goal_completions_all_goals_mom,
max(goal_conversion_rate_all_goals_mom) as goal_conversion_rate_all_goals_mom,
max(bounce_rate_mom) bounce_rate_mom,
max(avg_seconds_on_site_mom) as avg_seconds_on_site_mom,
sum(sessions_yoy) as sessions_yoy,
sum(transaction_revenue_yoy) transaction_revenue_yoy,
sum(transactions_yoy) as transactions_yoy,
max(ecommerce_conversion_rate_yoy) as ecommerce_conversion_rate_yoy,
sum(goal_completions_all_goals_yoy) as goal_completions_all_goals_yoy,
max(goal_conversion_rate_all_goals_yoy) as goal_conversion_rate_all_goals_yoy,
max(bounce_rate_yoy) bounce_rate_yoy,
max(avg_seconds_on_site_yoy) as avg_seconds_on_site_yoy
FROM (

	SELECT
	date_from_unix_date(unix_run_date) date,
	account,
	site,
	domain,
	url,
	case when unix_date = unix_run_date then sessions else 0 end as sessions_30d,
	case when unix_date = unix_run_date then transaction_revenue else 0 end as transaction_revenue_30d,
	case when unix_date = unix_run_date then transactions else 0 end as transactions_30d,
	case when unix_date = unix_run_date then ecommerce_conversion_rate else 0 end as ecommerce_conversion_rate_30d,
	case when unix_date = unix_run_date then goal_completions_all_goals else 0 end as goal_completions_all_goals_30d,
	case when unix_date = unix_run_date then goal_conversion_rate_all_goals else 0 end as goal_conversion_rate_all_goals_30d,
	case when unix_date = unix_run_date then bounce_rate else 0 end as bounce_rate_30d,
	case when unix_date = unix_run_date then avg_seconds_on_site else 0 end as avg_seconds_on_site_30d,
	null as sessions_mom,
	null as transaction_revenue_mom,
	null as transactions_mom,
	null as ecommerce_conversion_rate_mom,
	null as goal_completions_all_goals_mom,
	null as goal_conversion_rate_all_goals_mom,
	null as bounce_rate_mom,
	null as avg_seconds_on_site_mom,
	null as sessions_yoy,
	null as transaction_revenue_yoy,
	null as transactions_yoy,
	null as ecommerce_conversion_rate_yoy,
	null as goal_completions_all_goals_yoy,
	null as goal_conversion_rate_all_goals_yoy,
	null as bounce_rate_yoy,
	null as avg_seconds_on_site_yoy
	FROM ga

	UNION ALL	

	SELECT
	date_from_unix_date(unix_run_date) date,
	account,
	site,
	domain,
	url,
	null as sessions_30d,
	null as transaction_revenue_30d,
	null as transactions_30d,
	null as ecommerce_conversion_rate_30d,
	null as goal_completions_all_goals_30d,
	null as goal_conversion_rate_all_goals_30d,
	null as bounce_rate_30d,
	null as avg_seconds_on_site_30d,	
	case when unix_date = unix_mom_date then sessions else 0 end as sessions_mom,
	case when unix_date = unix_mom_date then transaction_revenue else 0 end as transaction_revenue_mom,
	case when unix_date = unix_mom_date then transactions else 0 end as transactions_mom,
	case when unix_date = unix_mom_date then ecommerce_conversion_rate else 0 end as ecommerce_conversion_rate_mom,
	case when unix_date = unix_mom_date then goal_completions_all_goals else 0 end as goal_completions_all_goals_mom,
	case when unix_date = unix_mom_date then goal_conversion_rate_all_goals else 0 end as goal_conversion_rate_all_goals_mom,
	case when unix_date = unix_mom_date then bounce_rate else 0 end as bounce_rate_mom,
	case when unix_date = unix_mom_date then avg_seconds_on_site else 0 end as avg_seconds_on_site_mom,
	null as sessions_yoy,
	null as transaction_revenue_yoy,
	null as transactions_yoy,
	null as ecommerce_conversion_rate_yoy,
	null as goal_completions_all_goals_yoy,
	null as goal_conversion_rate_all_goals_yoy,
	null as bounce_rate_yoy,
	null as avg_seconds_on_site_yoy
	FROM ga	

	UNION ALL

	SELECT
	date_from_unix_date(unix_run_date) date,
	account,
	site,
	domain,
	url,
	null as sessions_30d,
	null as transaction_revenue_30d,
	null as transactions_30d,
	null as ecommerce_conversion_rate_30d,
	null as goal_completions_all_goals_30d,
	null as goal_conversion_rate_all_goals_30d,
	null as bounce_rate_30d,
	null as avg_seconds_on_site_30d,	
	null as sessions_mom,
	null as transaction_revenue_mom,
	null as transactions_mom,
	null as ecommerce_conversion_rate_mom,
	null as goal_completions_all_goals_mom,
	null as goal_conversion_rate_all_goals_mom,
	null as bounce_rate_mom,
	null as avg_seconds_on_site_mom,
	case when unix_date = unix_yoy_date then sessions else 0 end as sessions_yoy,
	case when unix_date = unix_yoy_date then transaction_revenue else 0 end as transaction_revenue_yoy,
	case when unix_date = unix_yoy_date then transactions else 0 end as transactions_yoy,
	case when unix_date = unix_yoy_date then ecommerce_conversion_rate else 0 end as ecommerce_conversion_rate_yoy,
	case when unix_date = unix_yoy_date then goal_completions_all_goals else 0 end as goal_completions_all_goals_yoy,
	case when unix_date = unix_yoy_date then goal_conversion_rate_all_goals else 0 end as goal_conversion_rate_all_goals_yoy,
	case when unix_date = unix_yoy_date then bounce_rate else 0 end as bounce_rate_yoy,
	case when unix_date = unix_yoy_date then avg_seconds_on_site else 0 end as avg_seconds_on_site_yoy
	FROM ga		
)
group by date, account, site, domain, url
