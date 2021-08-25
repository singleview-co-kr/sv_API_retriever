SELECT `ad_keyword`
FROM `nvad_master_keyword`
WHERE `ad_keyword_id` = %s AND DATE(DATE_ADD(`regTm`, INTERVAL -1 DAY)) <= %s
ORDER BY `keyword_srl` DESC