INSERT INTO `nvad_brs_contract`
(`contract_id`, `contract_status`, `contract_regdate`, `contract_name`, `conntected_ad_group`,
 `template_name`, `available_queries`, `contract_date_begin`, `contract_date_end`, `contract_amnt`,
 `refund_amnt`, `ua`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)