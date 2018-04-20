SELECT 
url,
http_status_code
FROM {{ ref('deepcrawl_class') }}