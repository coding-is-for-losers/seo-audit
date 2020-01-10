with deepcrawl as (

SELECT
    site,
    crawl_id,
    lag(crawl_id) over w1 previous_crawl_id,
    lead(crawl_id) over w1 next_crawl_id,
    crawl_report_month
    FROM (
      SELECT
      site,
      crawl_id,
      crawl_report_month
      FROM {{ ref('deepcrawl_proc') }}
      GROUP BY site, crawl_id, crawl_report_month
    )
    WINDOW w1 as (PARTITION BY site ORDER BY crawl_id asc)
)

SELECT
site,
run_date,
max(crawl_id_proc) crawl_id,
max(crawl_report_month_proc) crawl_report_month
FROM (

  SELECT
  site,
  run_date,
  crawl_report_month,
  CASE WHEN run_date = crawl_report_month THEN crawl_id
    WHEN crawl_report_month < run_date THEN crawl_id
    WHEN crawl_report_month > run_date and previous_crawl_id is null THEN crawl_id
    ELSE null END as crawl_id_proc,
  CASE WHEN run_date = crawl_report_month THEN crawl_report_month
    WHEN crawl_report_month < run_date THEN crawl_report_month
    WHEN crawl_report_month > run_date and previous_crawl_id is null THEN crawl_report_month
    ELSE null END as crawl_report_month_proc
  FROM (

    SELECT
    a.site,
    a.run_date,
    b.crawl_id,
    b.previous_crawl_id,
    b.next_crawl_id,
    b.crawl_report_month
    FROM {{ ref('dates')}} a
    JOIN deepcrawl b
    ON (
      a.site = b.site
    )
  )
)
GROUP BY site, run_date