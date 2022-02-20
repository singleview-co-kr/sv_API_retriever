SELECT `id`, `qty`, `amnt` 
FROM `edi_emart_daily_log`
WHERE `item_id` = %s AND `branch_id` = %s AND `logdate` = %s