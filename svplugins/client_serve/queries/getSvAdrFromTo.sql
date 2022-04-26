SELECT `document_srl`, `addr_full`, `addr_do`, `addr_si`, `addr_gu_gun`, `addr_dong_myun_eup`, `logdate`
FROM `sv_adr_log`
WHERE `logdate` >= %s and `logdate` <= %s