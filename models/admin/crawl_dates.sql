with deepcrawl as (

		SELECT
		site,
		crawl_date,
		crawl_report_month
		FROM {{ ref('deepcrawl_proc') }}
    GROUP BY site, crawl_date, crawl_report_month
)

SELECT
site,
run_date,
min(crawl_date) crawl_date,
min(crawl_report_month) crawl_report_month
FROM (

  SELECT
  site,
  run_date,
  CASE WHEN crawl_date is not null THEN crawl_date
    WHEN crawl_date_before_run_date is not null THEN crawl_date_before_run_date
    WHEN crawl_date_after_run_date is not null THEN crawl_date_after_run_date  
    ELSE null END as crawl_date,
  CASE WHEN crawl_report_month is not null THEN crawl_report_month
    WHEN crawl_report_month_before_run_date is not null THEN crawl_report_month_before_run_date
    WHEN crawl_report_month_after_run_date is not null THEN crawl_report_month_after_run_date  
    ELSE null END as crawl_report_month
  FROM (
      SELECT
      a.site,
      a.run_date,
      b.crawl_date,
      b.crawl_report_month,
      c.crawl_date crawl_date_before_run_date,
      c.crawl_report_month crawl_report_month_before_run_date,
      d.crawl_date crawl_date_after_run_date,
      d.crawl_report_month crawl_report_month_after_run_date
      FROM {{ ref('dates') }} a
      LEFT JOIN deepcrawl b
      ON (
        a.site = b.site and
        a.run_date = b.crawl_report_month
      )
      LEFT JOIN deepcrawl c
      ON (
        a.site = c.site and
        a.run_date > c.crawl_report_month
      )
      LEFT JOIN deepcrawl d
      ON (
        a.site = d.site and
        a.run_date < d.crawl_report_month
      )
    )
)
GROUP BY site, run_date
ORDER BY run_date desc
  