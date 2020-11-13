WITH ga_domain as (

    SELECT 
    view as google_analytics_account,
    hostname as domain
    FROM (
      SELECT
      date_of_entry,
      view,
      hostname,
      sessions,
      total_transactions,
      first_value(date_of_entry) OVER (PARTITION BY view ORDER BY date_of_entry DESC) lv
      FROM (

        SELECT 
        cast(time_of_entry as date) date_of_entry,
        view,
        hostname,
        sum(sessions) as sessions,
        sum(transactions) as total_transactions,
        FROM `{{ target.project }}.{{ target.schema }}.ga`
        GROUP BY date_of_entry,view,hostname
        )  
    )
    where lv = date_of_entry and (sessions > 250 or total_transactions>1)

)

SELECT
a.site,
b.domain,
a.search_console_account,
a.google_analytics_account
FROM (
    SELECT
    site,
    domain,
    search_console_account,
    google_analytics_account
    FROM (

      SELECT
      site_name site,
      trim(replace(replace(replace(full_homepage_url,'www.',''),'http://',''),'https://',''),'/') domain,
      google_search_console_account search_console_account,
      google_analytics_account,
      time_of_entry,
      first_value(time_of_entry) OVER (PARTITION BY site_name ORDER BY time_of_entry DESC) lv
      FROM `{{ target.project }}.{{ target.schema }}.sites`
      WHERE site_name is not null
    )
    WHERE lv = time_of_entry
  ) a
LEFT JOIN ga_domain b
ON ( 
  a.google_analytics_account = b.google_analytics_account
  )
