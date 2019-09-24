WITH ga as (
	SELECT
	account,
	site,
	date,
	count(url) ga_count,
	null as gsc_count
	FROM {{ref('ga_proc')}} 
	GROUP BY account, site, date
),

gsc as (
	SELECT
	account,
	site,
	date,
	null as ga_count,
	count(url) as gsc_count
	FROM {{ref('search_console_url_proc')}} 
	GROUP BY account, site, date
)


SELECT
site,
run_date,
mom_date,
yoy_date,
unix_date(run_date) unix_run_date,
unix_date(mom_date) unix_mom_date,
unix_date(yoy_date) unix_yoy_date
FROM (

	SELECT
	site,
	date run_date,
	date_sub(max(date), INTERVAL 1 MONTH) mom_date,
	date_sub(max(date), INTERVAL 12 MONTH) yoy_date
	FROM (
	  SELECT
	  site,
	  date,
	  sum(ga_count) ga_count,
	  sum(gsc_count) gsc_count
	  from (
	    SELECT * FROM ga
	    UNION ALL
	    SELECT * FROM gsc
	    )
	  GROUP BY site, date 
	)
	WHERE ga_count > 0
	AND gsc_count > 0 
	GROUP BY site, date
)