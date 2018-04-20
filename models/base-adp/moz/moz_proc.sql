SELECT  
account,
'Moz' as platform,
date,
max(domain_authority) as domain_authority
FROM {{var('moz')}}
where account in (select account from {{ref('accounts_proc')}} where platform = 'Moz')
group by account, platform, date
order by account asc, date asc