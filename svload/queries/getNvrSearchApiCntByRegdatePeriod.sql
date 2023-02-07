SELECT `morpheme_srl`, `media_id`, `regdate` as `logdate`
FROM `nvsearch_log`
WHERE `regdate` >= %s AND `regdate` <= %s AND `media_id` = %s