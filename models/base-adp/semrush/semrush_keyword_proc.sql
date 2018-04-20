SELECT  
date, 
unix_date(date) unix_date,
account,
'SEMrush' as platform,
lower(trim(trim(regexp_replace(replace(replace(replace(url,'www.',''),'http://',''),'https://',''),r'\?.*$',''),' '),'/')) as url,
keyword,
min(position) as position, 
min(previous_position) as previous_position, 
max(search_volume) as search_volume,
max(cpc) as cpc
FROM {{var('semrush_keyword')}}
where account in (select account from {{ref('accounts_proc')}} where platform = 'SEMrush')
and keyword not in {{var('brand_keywords')}}
group by date, unix_date, account, platform, url, keyword