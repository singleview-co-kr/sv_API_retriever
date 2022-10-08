UPDATE `nvad_brs_contract`
SET `contract_status` = %s, `contract_date_end` = %s, `refund_amnt` = %s, `ua` = %s
WHERE `contract_srl` = %s