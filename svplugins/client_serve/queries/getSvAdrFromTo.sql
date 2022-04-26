SELECT `document_srl`, `addr_full`, `logdate`
FROM `sv_adr_log`
WHERE `logdate` >= %s and `logdate` <= %s