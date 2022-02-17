DELETE
FROM `compiled_ga_media_daily_log`
WHERE `logdate` >= %s AND `logdate` <= %s