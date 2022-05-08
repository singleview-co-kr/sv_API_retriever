SELECT `log_srl`, `document_srl`, `addr_raw`, `logdate`
FROM `sv_adr_log`
WHERE `module_srl`=%s AND `postcode`=%s AND `document_srl`!=%s