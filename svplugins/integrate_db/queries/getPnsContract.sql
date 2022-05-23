SELECT *
FROM `pns_contract`
WHERE `source_id` = %s AND `execute_date_begin` <= %s AND `execute_date_end` >= %s