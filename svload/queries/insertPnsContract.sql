INSERT INTO `pns_contract`
(`source_id`, `contract_type`, `media_term`, `contractor_id`, `cost_incl_vat`, 
 `agency_rate_percent`, `execute_date_begin`, `execute_date_end`, `regdate`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)