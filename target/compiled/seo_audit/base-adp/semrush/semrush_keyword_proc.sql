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
FROM `curious-domain-121318.seo_audit.semrush_keyword`
where account in (select account from `curious-domain-121318`.`seo_audit`.`accounts_proc` where platform = 'SEMrush')
and keyword not in ('cifl', 'losers', 'coding is for losers')
group by date, unix_date, account, platform, url, keyword