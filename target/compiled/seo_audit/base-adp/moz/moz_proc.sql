SELECT  
account,
'Moz' as platform,
date,
max(domain_authority) as domain_authority
FROM `curious-domain-121318.seo_audit.moz`
where account in (select account from `curious-domain-121318`.`seo_audit`.`accounts_proc` where platform = 'Moz')
group by account, platform, date
order by account asc, date asc