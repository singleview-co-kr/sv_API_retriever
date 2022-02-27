SELECT count(*)
FROM `edi_ltmart_daily_log`
WHERE `logdate` >= %s AND `logdate` <= %s