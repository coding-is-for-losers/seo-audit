SELECT
*
FROM
{{ ref('deepcrawl_rules_query_string') }}
order by pct_of_value desc
limit 1
