SELECT
b.client client,
url,
title,
title_length,
description,
description_length,
word_count,
status_code,
meta_robots,
canonical_link_element,
content
FROM 

(

	SELECT  
	filename as account,
	'ScreamingFrog' as platform,
	lower(trim(regexp_replace(replace(replace(replace(url,'www.',''),'http://',''),'https://',''),r'\?.*$',''),'/')) as url,
	title,
	title_length,
	description,
	description_length,
	word_count,
	status_code,
	meta_robots,
	canonical_link_element,
	content,
	time_of_entry,
	first_value(time_of_entry) OVER (PARTITION BY filename ORDER BY time_of_entry DESC) lv
	FROM `curious-domain-121318.seo_audit.screamingfrog` 
	where filename in (select account from `curious-domain-121318`.`seo_audit`.`accounts_proc` where platform = 'ScreamingFrog')
	and content = 'text/html; charset=UTF-8'

) a
LEFT JOIN `curious-domain-121318`.`seo_audit`.`accounts_proc` b
ON ( a.account = b.account
	and a.platform = b.platform )
WHERE time_of_entry = lv