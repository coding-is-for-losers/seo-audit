with ga as (
	select
	account,
	client,
	platform,
	date,
	count(url) ga_count,
	null as ga_pageviews_count,
	null as gsc_count
	from `curious-domain-121318`.`seo_audit`.`ga_proc`
	group by account, platform, date
),

ga_pageviews (
	select
	account,
	client,
	platform,
	date,
	null as ga_count,
	count(url) ga_pageviews_count,
	null as gsc_count
	from `curious-domain-121318`.`seo_audit`.`ga_proc_pageviews`
	group by account, platform, date
),

gsc as (
	select
	account,
	client,
	platform,
	date,
	null as ga_count,
	null as ga_pageviews_count,
	count(url) as gsc_count
	from `curious-domain-121318`.`seo_audit`.`search_console_proc`
	group by account, platform, date
)


select
client,
max(date) run_date,
unix_date(max(date)) unix_run_date
from (
	select * from ga
	union all
	select * from ga_pageviews
	union all
	select * from gsc
	)
where ga_count > 0
and ga_pageviews_count > 0
and gsc_count > 0 
group by client