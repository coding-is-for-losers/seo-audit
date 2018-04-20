SELECT
date,
unix_date,
account,
client,
platform,
url,
sum(sessions) sessions,
sum(revenue) revenue,
sum(transactions) transactions

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
	sessions,
	revenue,
	transactions
	FROM

	( 

		SELECT 
		date,
		unix_date,
		account,
		platform,
		lower(trim(replace(replace(replace(CONCAT(sessions_hostname,path),'www.',''),'http://',''),'https://',''),'/')) url,
		lower(trim(regexp_replace(replace(replace(replace(CONCAT(sessions_hostname,path),'www.',''),'http://',''),'https://',''),r'\?.*$',''),'/')) url_stripped,
		sum(sessions) sessions,
		sum(revenue) revenue,
		sum(transactions) transactions
		FROM (
			SELECT 
			date,
			unix_date(date) unix_date,
			account,
			'Google Analytics' as platform,
			hostname,
			first_value(hostname) OVER (PARTITION BY path ORDER BY max(sessions) desc) sessions_hostname,
			path,
			source,
			medium,
			max(sessions) sessions,
			max(leads) revenue,
			max(transactions) transactions
			FROM `curious-domain-121318.seo_audit.ga` 
			where account in (select account from `curious-domain-121318`.`seo_audit`.`accounts_proc` where platform = 'Google Analytics')
			and path != "(not set)"
			and medium = 'organic'
			group by date, account, platform, source, medium, hostname, path
			)
		group by date, unix_date, account, platform, url, url_stripped
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
	date, unix_date, account, client, platform, url