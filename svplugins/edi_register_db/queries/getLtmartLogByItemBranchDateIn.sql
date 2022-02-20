SELECT `id`, `qty`, `amnt`
FROM `edi_ltmart_daily_log`
WHERE `item_id` = %s AND `branch_id` = %s AND `logdate_since` <= %s AND `logdate` >= %s