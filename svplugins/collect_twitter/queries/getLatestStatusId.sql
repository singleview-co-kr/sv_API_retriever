SELECT MAX(`status_id`) as status_id
FROM `twt_status`
WHERE `morpheme_srl` = %s