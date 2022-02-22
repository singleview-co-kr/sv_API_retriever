SELECT `item_id`, `branch_id`, `qty`, `amnt`, `logdate`
FROM `edi_emart_daily_log`
WHERE `logdate` BETWEEN %s AND %s