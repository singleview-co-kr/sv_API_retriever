INSERT INTO `sv_adr_log` 
(`document_srl`, `module_srl`, `postcode`, 
 `addr_do`, `addr_si`, `addr_gu_gun`, `addr_dong_myun_eup`,
 `addr_full`, `addr_raw`, `logdate`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)