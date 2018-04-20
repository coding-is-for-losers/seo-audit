select 
client,
account,
platform platform,
max(time_of_entry)

from  ( 

SELECT  
client,
account,
platform,
time_of_entry,
first_value(time_of_entry) OVER (PARTITION BY client, platform ORDER BY time_of_entry DESC) lv
FROM `curious-domain-121318.seo_audit.accounts`

) 

WHERE lv = time_of_entry
group by client, account, platform
order by client asc, account asc