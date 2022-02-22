SELECT `item_id`, `branch_id`, `qty`, `amnt`, `logdate_since`, `logdate`
FROM `edi_ltmart_daily_log`
WHERE `logdate` BETWEEN %s AND %s