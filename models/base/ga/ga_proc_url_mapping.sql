SELECT
site, 
domain,
account,
date,
unix_date,
url,
sum(sessions) sessions,
sum(transaction_revenue) transaction_revenue,
sum(transactions) transactions,
CASE WHEN sum(sessions) > 0 THEN sum(transactions) / sum(sessions) ELSE 0 END as ecommerce_conversion_rate,
sum(goal_completions_all_goals) goal_completions_all_goals,
CASE WHEN sum(sessions) > 0 THEN sum(goal_completions_all_goals) / sum(sessions) ELSE 0 END as goal_conversion_rate_all_goals,
CASE WHEN sum(sessions) > 0 THEN sum(bounces) / sum(sessions) ELSE null END as bounce_rate,
CASE WHEN sum(sessions) > 0 THEN sum(seconds_on_site)/sum(sessions) ELSE null END as avg_seconds_on_site
FROM (

    SELECT
    a.site, 
    a.domain,
    a.account,
    a.date,
    unix_date,
    CASE WHEN b.url is not null THEN a.url
        WHEN regexp_contains(a.url, r'^.*\/([^\/]+?\.[^\/]+)$') THEN regexp_replace(a.url, r'\?.*$', '')
        ELSE concat(trim(regexp_replace(a.url, r'\?.*$',''),'/'),'/') END as url,        
    sessions,
    transaction_revenue,
    transactions,
    goal_completions_all_goals,
    seconds_on_site,
    bounces
    FROM {{ ref('ga_proc')}} a
    LEFT JOIN {{ ref('deepcrawl_proc')}} b
    ON (
        a.date = b.crawl_report_month AND 
        a.site = b.site AND 
        a.url = b.url )
)
GROUP BY site, domain, account, date, unix_date, url