DELETE
FROM `compiled_ga_media_daily_log`
WHERE YEAR(logdate) = %s AND MONTH(logdate) = %s