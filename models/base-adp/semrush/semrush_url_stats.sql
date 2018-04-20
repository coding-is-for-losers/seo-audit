WITH history AS (
	SELECT * FROM {{ref('semrush_url_history')}}
),
daterange AS (
	SELECT * FROM {{ref('all_dates')}}  	
),
temp AS (
  SELECT account, url, platform, date d,
  semrush_keyword_count, semrush_total_cpc, semrush_total_search_volume, semrush_min_position,
  LEAD(date) OVER(PARTITION BY account, url, platform ORDER BY date) AS next_date
  FROM history
  ORDER BY account, platform, url, date
)
SELECT date_in_range, account, platform, url, 
semrush_keyword_count, semrush_total_cpc, semrush_total_search_volume, semrush_min_position
FROM daterange
JOIN temp
ON daterange.date_in_range >= temp.d
AND (daterange.date_in_range < temp.next_date OR temp.next_date IS NULL)
ORDER BY account, platform, url, date_in_range