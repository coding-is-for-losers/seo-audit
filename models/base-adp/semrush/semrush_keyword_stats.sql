WITH history AS (
	SELECT * FROM {{ref('semrush_keyword_history')}}
),
daterange AS (
	SELECT * FROM {{ref('all_dates')}}  	
),
temp AS (
  SELECT account, url, platform, date d, semrush_top_keyword_vol, semrush_top_keyword_cpc,
  semrush_top_keyword_vol_vol, semrush_top_keyword_vol_cpc, semrush_top_keyword_cpc_vol, semrush_top_keyword_cpc_cpc,
  LEAD(date) OVER(PARTITION BY account, url, platform ORDER BY date) AS next_date
  FROM history
  ORDER BY account, platform, url, date
)
SELECT date_in_range, account, platform, url, 
semrush_top_keyword_vol, semrush_top_keyword_cpc,
semrush_top_keyword_vol_vol, semrush_top_keyword_vol_cpc, semrush_top_keyword_cpc_vol, semrush_top_keyword_cpc_cpc
FROM daterange
JOIN temp
ON daterange.date_in_range >= temp.d
AND (daterange.date_in_range < temp.next_date OR temp.next_date IS NULL)