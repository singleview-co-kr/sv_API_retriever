SELECT `contract_srl`, `contract_id`, `conntected_ad_group`, `available_queries`, `contract_date_begin`, `contract_date_end`, `contract_amnt`, `refund_amnt`, `ua`
FROM `nvad_brs_contract`
WHERE `contract_date_begin` >= %s AND `contract_date_end` <= %s
ORDER BY `contract_srl` desc