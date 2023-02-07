SELECT `morpheme_srl`, `logdate`
FROM `twt_status`
WHERE `logdate` >= %s AND `logdate` <= %s