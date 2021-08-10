DELETE
FROM `gross_compiled_daily_log`
WHERE `logdate` >= %s AND `logdate` <= %s