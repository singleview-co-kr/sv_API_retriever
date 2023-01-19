SELECT `contract_srl`
FROM `nvad_brs_contract`
WHERE `contract_date_begin` <= %s AND `contract_date_end` >= %s AND `ua` = %s