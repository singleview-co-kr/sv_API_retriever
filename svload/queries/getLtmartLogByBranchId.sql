SELECT `item_id`, `branch_id`, `qty`, `amnt`, `logdate`
FROM `edi_ltmart_daily_log`
WHERE `logdate` BETWEEN %s AND %s AND `branch_id` = %s