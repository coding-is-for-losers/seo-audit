SELECT 
account, 
platform, 
url,
date, 
SUM(ref_domain_count) OVER(PARTITION BY account, url ORDER BY date asc) as ref_domain_count,
SUM(citation_flow) OVER(PARTITION BY account, url ORDER BY date asc) as citation_flow_sum,
SUM(trust_flow) OVER(PARTITION BY account, url ORDER BY date asc) as trust_flow_sum
FROM (

SELECT  
account,
platform,
target_url url,
date,
count(source_domain) as ref_domain_count,
sum(citation_flow) as citation_flow,
sum(trust_flow) as trust_flow
FROM `curious-domain-121318`.`seo_audit`.`majestic_domain_proc`
group by account, platform, date, url

)