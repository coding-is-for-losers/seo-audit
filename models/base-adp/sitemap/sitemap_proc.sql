SELECT 
a.sitemap,
b.client,
url
FROM 
(

	SELECT  
	sitemap as account,
	sitemap,
	'Sitemap' as platform,
	lower(trim(regexp_replace(replace(replace(replace(url,'www.',''),'http://',''),'https://',''),r'\?.*$',''),'/')) as url,
	time_of_entry,
	first_value(time_of_entry) OVER (PARTITION BY sitemap ORDER BY time_of_entry DESC) lv
	FROM {{var('sitemap')}} 
	where sitemap in (select account from {{ref('accounts_proc')}} where platform = 'Sitemap')

) a
LEFT JOIN {{ref('accounts_proc')}} b
ON ( a.account = b.account
	and a.platform = b.platform )
where time_of_entry = lv