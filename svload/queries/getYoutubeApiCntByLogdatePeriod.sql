SELECT `morpheme_srl`, `logdate`
FROM `ytsearch_log`
WHERE `logdate` >= %s AND `logdate` <= %s