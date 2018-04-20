SELECT
*
FROM
{{ ref('deepcrawl_rules_filename') }}
order by pct_of_value desc
limit 1
