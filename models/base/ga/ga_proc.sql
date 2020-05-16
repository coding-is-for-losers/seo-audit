SELECT 
site,
domain,
account,
date,
unix_date,
date_of_entry,
url,
sum(sessions) sessions,
sum(transaction_revenue) transaction_revenue,
sum(transactions) transactions,
sum(goal_completions_all_goals) goal_completions_all_goals,
sum(bounces) bounces,
sum(seconds_on_site) seconds_on_site
FROM (

    SELECT
    site,
    domain,
    account,
    date,
    unix_date,
    date_of_entry,
    url_untrimmed,
    first_value(url_untrimmed) over (PARTITION BY site, domain, account, date, url_trimmed ORDER BY sessions desc) url,
    replace(regexp_extract(url_trimmed,r'^(?:https?:\/\/)?(?:www\.)?([^\/]+)'),'(not set)','') as url_domain,
    sessions,
    transaction_revenue,
    transactions,
    goal_completions_all_goals,
    bounces,
    seconds_on_site
    FROM (

        SELECT
        b.site, 
        b.domain,
        account,
        month date,
        unix_date,
        date_of_entry,
        CASE WHEN regexp_contains(landing_page_path,domain) 
          THEN lower(regexp_replace(replace(replace(replace(landing_page_path,'www.',''),'http://',''),'https://',''),r'\#.*$',''))
          ELSE lower(regexp_replace(replace(replace(replace(CONCAT(a.hostname,landing_page_path),'www.',''),'http://',''),'https://',''),r'\#.*$',''))
          END as url_untrimmed,
        CASE WHEN regexp_contains(landing_page_path,domain) 
          THEN trim(lower(regexp_replace(replace(replace(replace(landing_page_path,'www.',''),'http://',''),'https://',''),r'\#.*$','')),'/')
          ELSE trim(lower(regexp_replace(replace(replace(replace(CONCAT(a.hostname,landing_page_path),'www.',''),'http://',''),'https://',''),r'\#.*$','')),'/')
          END as url_trimmed,          
        sum(sessions) sessions,
        sum(transaction_revenue) transaction_revenue,
        sum(transactions) transactions,
        sum(goal_completions_all_goals) goal_completions_all_goals,
        sum(bounces) bounces,
        sum(seconds_on_site) seconds_on_site
        FROM (
                SELECT 
                rtrim(view) as account,
                month,
                unix_date(month) unix_date,
                time_of_entry,
                cast(time_of_entry as date) date_of_entry,
                last_value(time_of_entry) OVER (PARTITION BY view, landing_page_path, hostname, month ORDER BY time_of_entry desc) lv,
                replace(hostname,'www.','') hostname,
                trim(replace(hostname,'www.',''),'/') hostname_trimmed,
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
            a.hostname_trimmed = b.domain
        )
        WHERE time_of_entry = lv
        GROUP BY b.site, domain, account, month, unix_date, date_of_entry, url_untrimmed, url_trimmed
    )
)
WHERE domain = url_domain
GROUP BY site, domain, account, date, unix_date, date_of_entry, url