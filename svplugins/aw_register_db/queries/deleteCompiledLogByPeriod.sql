DELETE
FROM `aw_compiled_daily_log`
WHERE `logdate` >= %s AND `logdate` <= %s