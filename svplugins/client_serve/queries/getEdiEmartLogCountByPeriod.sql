SELECT count(*)
FROM `edi_emart_daily_log`
WHERE `logdate` BETWEEN %s AND %s