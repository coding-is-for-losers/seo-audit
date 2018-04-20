select 
date,
unix_date,
a.account,
b.client,
a.platform,
url,
keyword,
impressions,
clicks,
position
FROM (

	SELECT  
	date, 
	unix_date(date) as unix_date,
	site as account,
	'Organic' as platform,
	lower(trim(regexp_replace(replace(replace(replace(url,'www.',''),'http://',''),'https://',''),r'\?.*$',''),'/')) as url,
	keyword,
	max(impressions) as impressions, 
	max(clicks) as clicks, 
	min(position) as position
	FROM `curious-domain-121318.seo_audit.gsc` 
	group by date, unix_date, account, platform, url, keyword
	) a
LEFT JOIN `curious-domain-121318`.`seo_audit`.`accounts_proc` b
ON (
	a.platform = b.platform and
	a.account = b.account
)