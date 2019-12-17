with deepcrawl as (

		SELECT
		site,
		crawl_id,
		crawl_report_month
		FROM {{ ref('deepcrawl_proc') }}
    GROUP BY site, crawl_id, crawl_report_month
)

SELECT
site,
run_date,
min(crawl_id) crawl_id,
min(crawl_report_month) crawl_report_month
FROM (

  SELECT
  site,
  run_date,
  CASE WHEN crawl_id is not null THEN crawl_id
    WHEN crawl_id_before_run_date is not null THEN crawl_id_before_run_date
    WHEN crawl_id_after_run_date is not null THEN crawl_id_after_run_date  
    ELSE null END as crawl_id,
  CASE WHEN crawl_report_month is not null THEN crawl_report_month
    WHEN crawl_report_month_before_run_date is not null THEN crawl_report_month_before_run_date
    WHEN crawl_report_month_after_run_date is not null THEN crawl_report_month_after_run_date  
    ELSE null END as crawl_report_month
  FROM (
      SELECT
      a.site,
      a.run_date,
      b.crawl_id,
      b.crawl_report_month,
      c.crawl_id crawl_id_pre_run_date,
      c.crawl_report_month crawl_report_month_pre_run_date,
      d.crawl_id crawl_id_post_run_date,
      d.crawl_report_month crawl_report_month_post_run_date
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
  