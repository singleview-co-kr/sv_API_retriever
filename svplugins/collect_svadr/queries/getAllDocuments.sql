SELECT `log_srl`, `document_srl`, `postcode`, `addr_do`, `addr_si`, `addr_gu_gun`, `addr_dong_myun_eup`, `addr_raw`, `logdate`
FROM `sv_adr_log`
WHERE `module_srl`=%s