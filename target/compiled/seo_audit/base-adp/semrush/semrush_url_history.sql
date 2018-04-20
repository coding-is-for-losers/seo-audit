SELECT  
date, 
account,
platform,
url,
count(distinct(keyword)) semrush_keyword_count,
sum(cpc) semrush_total_cpc,
sum(search_volume) semrush_total_search_volume
FROM `curious-domain-121318`.`seo_audit`.`semrush_keyword_proc`
where account in (select account from `curious-domain-121318`.`seo_audit`.`accounts_proc` where platform = 'SEMrush')
group by date, account, platform, url