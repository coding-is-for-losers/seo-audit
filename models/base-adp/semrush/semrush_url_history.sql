SELECT  
date, 
account,
platform,
url,
count(distinct(keyword)) semrush_keyword_count,
sum(cpc) semrush_total_cpc,
sum(search_volume) semrush_total_search_volume
FROM {{ref('semrush_keyword_proc')}}
where account in (select account from {{ref('accounts_proc')}} where platform = 'SEMrush')
group by date, account, platform, url