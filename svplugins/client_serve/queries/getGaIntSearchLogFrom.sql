SELECT `word`, `ua`, `cnt`, `logdate`
FROM `ga_intsearch_daily_log`
WHERE `logdate` >= %s