SELECT `log_srl`, `link`
FROM `nvsearch_log`
WHERE `logdate` IS NULL AND `b_crawled`=0 AND `media_id` = 5
LIMIT 300