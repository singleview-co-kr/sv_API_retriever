SELECT count(*)
FROM `edi_emart_daily_log`
WHERE `logdate` >= %s AND `logdate` <= %s