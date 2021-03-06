WITH history AS (
	SELECT * FROM `curious-domain-121318`.`seo_audit`.`semrush_url_history`
),
daterange AS (
	SELECT * FROM `curious-domain-121318`.`seo_audit`.`all_dates`  	
),
temp AS (
  SELECT account, url, platform, date d,
  semrush_keyword_count, semrush_total_cpc, semrush_total_search_volume,
  LEAD(date) OVER(PARTITION BY account, url, platform ORDER BY date) AS next_date
  FROM history
  ORDER BY account, platform, url, date
)
SELECT date_in_range, account, platform, url, 
semrush_keyword_count, semrush_total_cpc, semrush_total_search_volume
FROM daterange
JOIN temp
ON daterange.date_in_range >= temp.d
AND (daterange.date_in_range < temp.next_date OR temp.next_date IS NULL)
ORDER BY account, platform, url, date_in_range