with deepcrawl as (

  SELECT
  site,
  crawl_date,
  crawl_report_month,
  max(previous_crawl_report_month) previous_crawl_report_month,
  max(next_crawl_report_month) next_crawl_report_month
  FROM (

		SELECT
		site,
		crawl_date,
		crawl_report_month,
		lag(crawl_report_month, 1) over w1 as previous_crawl_report_month,
		lead(crawl_report_month, 1) over w1 as next_crawl_report_month
		FROM {{ ref('deepcrawl_proc') }}
		WINDOW w1 as (partition by domain, site order by crawl_report_month asc)
   )
   GROUP BY site, crawl_date, crawl_report_month
)

SELECT
site,
run_date,
max(crawl_date) crawl_date
FROM (

	SELECT
	a.site,
	a.run_date,
	CASE WHEN b.crawl_date is not null THEN b.crawl_date
		WHEN c.crawl_date is not null THEN c.crawl_date
		WHEN d.crawl_date is not null THEN d.crawl_date
		ELSE null END as crawl_date
	FROM {{ ref('dates')}} a
	LEFT JOIN deepcrawl b
	ON (
		a.site = b.site AND
		a.run_date = b.crawl_report_month
	)
	LEFT JOIN deepcrawl c
	ON (
		a.site = b.site AND
		a.run_date = c.previous_crawl_report_month
	)
	LEFT JOIN deepcrawl d
	ON (
		a.site = b.site AND
		a.run_date = d.next_crawl_report_month
	)
)
GROUP BY site, run_date