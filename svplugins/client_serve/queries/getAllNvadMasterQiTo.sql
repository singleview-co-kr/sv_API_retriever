SELECT `ad_group_id`, `ad_keyword`, `quality_index`, `check_date`
FROM `nvad_master_qi`
WHERE `check_date` <= %s