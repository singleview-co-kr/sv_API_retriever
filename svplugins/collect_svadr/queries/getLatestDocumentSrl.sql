SELECT MAX(`document_srl`) AS max_doc_srl
FROM `sv_adr_log`
WHERE `module_srl`=%s