SELECT  
account,
'Majestic' as platform,
source_domain,
lower(trim(regexp_replace(replace(replace(replace(target_url,'www.',''),'http://',''),'https://',''),r'\?.*$',''),'/')) as target_url,
min(date) as date, 
max(citation_flow) as citation_flow,
max(trust_flow) as trust_flow
FROM {{var('majestic')}} 
where account in (select account from {{ref('accounts_proc')}} where platform = 'Majestic')
and citation_flow > 20
group by account, platform, source_domain, target_url