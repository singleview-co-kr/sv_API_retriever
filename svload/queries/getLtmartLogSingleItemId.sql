SELECT `branch_id`, `qty`, `amnt`, `logdate`
FROM `edi_ltmart_daily_log`
WHERE `item_id` = %s AND `logdate` >= %s AND `logdate` <= %s