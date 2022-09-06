UPDATE `campaign_name_alias`
SET `source_id` = %s, `search_rst_id` = %s, `medium_id` = %s,
    `sv_lvl_1` = %s, `sv_lvl_2` = %s, `sv_lvl_3` = %s, 
    `sv_lvl_ext` = %s, `memo` = %s
WHERE `alias_id` = %s