with sessions as (
	SELECT 
	date, unix_date, account, client, platform, url, 
	sessions, revenue, transactions, null as pageviews
	FROM {{ ref('ga_proc') }}
),
pageviews as (
	SELECT 
	date, unix_date, account, client, platform, url, 
	null as sessions, null as revenue, null as transactions, pageviews
	FROM {{ ref('ga_proc_pageviews') }}
)

SELECT  
run_date,
account,
client,
platform,
url,
sum(case when unix_date >= ( unix_run_date - 30) and unix_date <= unix_run_date then sessions else 0 end ) as sessions_30d,
sum(case when unix_date >= unix_run_date - 30 and unix_date <= unix_run_date then revenue else 0 end ) as revenue_30d,
sum(case when unix_date >= unix_run_date - 30 and unix_date <= unix_run_date then transactions else 0 end ) as transactions_30d,
sum(case when unix_date >= unix_run_date - 30 and unix_date <= unix_run_date then pageviews else 0 end ) as pageviews_30d,
sum(case when unix_date >= (unix_run_date - 60) and unix_date < (unix_run_date - 30) then sessions else 0 end ) as sessions_mom,
sum(case when unix_date >= (unix_run_date - 60) and unix_date < (unix_run_date - 30) then revenue else 0 end ) as revenue_mom,
sum(case when unix_date >= (unix_run_date - 60) and unix_date < (unix_run_date - 30) then transactions else 0 end ) as transactions_mom,
sum(case when unix_date >= (unix_run_date - 60) and unix_date < (unix_run_date - 30) then pageviews else 0 end ) as pageviews_mom,
sum(case when unix_date >= (unix_run_date - 395) and unix_date < (unix_run_date - 365) then sessions else 0 end ) as sessions_yoy,
sum(case when unix_date >= (unix_run_date - 395) and unix_date < (unix_run_date - 365) then revenue else 0 end ) as revenue_yoy,
sum(case when unix_date >= (unix_run_date - 395) and unix_date < (unix_run_date - 365) then transactions else 0 end ) as transactions_yoy,
sum(case when unix_date >= (unix_run_date - 395) and unix_date < (unix_run_date - 365) then pageviews else 0 end ) as pageviews_yoy
FROM (

	select 
	a.date date,
    b.run_date run_date,
	unix_date,
	b.unix_run_date unix_run_date,
	a.account,
	a.client,
	a.platform,
	url,
	sum(sessions) sessions,
	sum(revenue) revenue,
	sum(transactions) transactions,
	sum(pageviews) pageviews
	FROM (
		SELECT * FROM sessions
		UNION ALL
		SELECT * FROM pageviews

	) a
	LEFT JOIN {{ ref('dates') }} b
	ON (
		a.client = b.client
		)
	group by date, run_date, unix_date, unix_run_date, account, client, platform, url

)
group by run_date, account, client, platform, url