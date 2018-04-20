WITH history AS (
	SELECT * FROM {{ref('majestic_domain_history')}}
),
daterange AS (
	SELECT * FROM {{ref('all_dates')}}  	
),
temp AS (
  SELECT account, url, platform, date d, ref_domain_count, citation_flow_sum, trust_flow_sum, 
  LEAD(date) OVER(PARTITION BY account, url, platform ORDER BY date) AS next_date
  FROM history
  ORDER BY account, platform, url, date
)
SELECT date_in_range, account, platform, url, ref_domain_count, citation_flow_sum/ref_domain_count as avg_citation_flow, trust_flow_sum/ref_domain_count as avg_trust_flow
FROM daterange
JOIN temp
ON daterange.date_in_range >= temp.d
AND (daterange.date_in_range < temp.next_date OR temp.next_date IS NULL)
ORDER BY account, platform, url, date_in_range