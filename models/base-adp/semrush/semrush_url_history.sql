SELECT  
date, 
account,
platform,
url,
count(distinct(keyword)) semrush_keyword_count,
sum(cpc) semrush_total_cpc,
sum(search_volume) semrush_total_search_volume,
min(position) semrush_min_position
FROM {{ref('semrush_keyword_proc')}}
where account in (select account from {{ref('accounts_proc')}} where platform = 'SEMrush')
group by date, account, platform, url