SELECT `log_srl`, `morpheme_srl`, `media_id`, `regdate`
FROM `nvsearch_log`
WHERE `regdate` >= %s AND `regdate` <= %s