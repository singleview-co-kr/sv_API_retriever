SELECT `contract_date_end`
FROM `nvad_brs_contract`
WHERE `ua` = %s
ORDER BY `contract_date_end` DESC LIMIT 1