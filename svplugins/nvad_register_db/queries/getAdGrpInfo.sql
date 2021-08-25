SELECT `ad_group_name`
FROM `nvad_master_ad_grp`
WHERE `ad_group_id` = %s AND DATE(DATE_ADD(`regTm`, INTERVAL -1 DAY)) <= %s
ORDER BY `ad_grp_srl` DESC