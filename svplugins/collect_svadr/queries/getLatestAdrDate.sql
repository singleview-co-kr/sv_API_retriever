SELECT MAX(`logdate`) AS maxdate
FROM `sv_adr_log`
WHERE `module_srl`=%s