SELECT *
FROM `pns_contract`
WHERE `execute_date_begin` >= %s AND `execute_date_end` <= %s
ORDER BY `contract_id` desc