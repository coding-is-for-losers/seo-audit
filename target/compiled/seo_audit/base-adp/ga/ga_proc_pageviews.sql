SELECT 
date,
unix_date,
account,
client,
platform,
url,
sum(pageviews) pageviews
FROM

( 
	SELECT 
	date,
	unix_date,
	a.account,
	c.client,
	a.platform,
	case when b.canonical_status in ( 'self', 'canonicalized', 'missing_canonical') then a.url 
		else a.url_stripped end as url,
	pageviews
	FROM

	( 
		SELECT 
		date,
		unix_date(date) unix_date,
		account,
		'Google Analytics' as platform,
		lower(trim(replace(replace(replace(CONCAT(hostname,path),'www.',''),'http://',''),'https://',''),'/')) as url,
		lower(trim(regexp_replace(replace(replace(replace(CONCAT(hostname,path),'www.',''),'http://',''),'https://',''),r'\?.*$',''),'/')) as url_stripped,
		max(pageviews) pageviews
		FROM `curious-domain-121318.seo_audit.ga_pageviews` 
		where account in ( select account from `curious-domain-121318`.`seo_audit`.`accounts_proc` where platform = 'Google Analytics')
		and path != "(not set)"
		group by date, account, platform, url, url_stripped

	) a
	LEFT JOIN `curious-domain-121318`.`seo_audit`.`agg_indicative` b
	ON (
		a.url = b.url
		)
	LEFT JOIN `curious-domain-121318`.`seo_audit`.`accounts_proc` c
	ON (
		a.platform = c.platform and
		a.account = c.account
	)
)
	
GROUP BY 
date, unix_date, client, account, platform, url