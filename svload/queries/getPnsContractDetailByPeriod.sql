SELECT *
FROM `pns_contract`
WHERE `contract_date_begin` >= %s AND `contract_date_end` <= %s
ORDER BY `contract_srl` desc