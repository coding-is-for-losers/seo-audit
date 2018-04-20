SELECT date_in_range
FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE('2016-01-01'), CURRENT_DATE(), INTERVAL 1 DAY)
) AS date_in_range