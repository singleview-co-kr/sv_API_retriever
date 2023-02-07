SELECT `morpheme_srl`, `media_id`, `logdate`
FROM `nvsearch_log`
WHERE `logdate` >= %s AND `logdate` <= %s AND `media_id` = %s