SELECT * FROM (

    SELECT
    site,
    domain,
    account,
    date,
    unix_date,
    date_of_entry,
    url,
    regexp_extract(url,r'^(?:https?:\/\/)?(?:www\.)?([^\/]+)') as url_domain,
    sessions,
    transaction_revenue,
    transactions,
    ecommerce_conversion_rate,
    goal_completions_all_goals,
    goal_conversion_rate_all_goals,
    bounce_rate,
    avg_seconds_on_site
    FROM (

        SELECT
        b.site, 
        b.domain,
        account,
        month date,
        unix_date,
        date_of_entry,
        CASE WHEN regexp_contains(landing_page_path,domain) 
          THEN lower(concat(trim(regexp_replace(regexp_replace(replace(replace(replace(landing_page_path,'www.',''),'http://',''),'https://',''),r'\?.*$',''),r'\#.*$',''),'/'),'/'))
          ELSE lower(concat(trim(regexp_replace(regexp_replace(replace(replace(replace(CONCAT(a.hostname,landing_page_path),'www.',''),'http://',''),'https://',''),r'\?.*$',''),r'\#.*$',''),'/'),'/'))
          END as url,
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
                rtrim(view) as account,
                month,
                unix_date(month) unix_date,
                time_of_entry,
                cast(time_of_entry as date) date_of_entry,
                first_value(time_of_entry) OVER (PARTITION BY view, landing_page_path, hostname, month ORDER BY time_of_entry desc) lv,
                replace(hostname,'www.','') hostname,
                landing_page_path,
                cast(sessions as int64) sessions,
                cast(transaction_revenue as int64) transaction_revenue,
                cast(transactions as int64) transactions,
                cast(goal_completions_all_goals as int64) goal_completions_all_goals,
                cast(bounces as int64) bounces,
                cast(seconds_on_site as int64) seconds_on_site
                FROM `{{ target.project }}.{{ target.schema }}.ga`
                WHERE ( sessions > 0 or transactions > 0 or goal_completions_all_goals > 0 )
                and landing_page_path not like '%.xml%'
                and landing_page_path != '(not set)'
                
                ) a
        LEFT JOIN {{ ref('domains_proc') }} b
        ON (
            a.account = b.google_analytics_account
        )
        WHERE time_of_entry = lv
        GROUP BY b.site, domain, account, month, unix_date, date_of_entry, url
    )
)
WHERE domain = url_domain