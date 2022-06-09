SELECT *
FROM `campaign_name_alias`
WHERE `regdate` >= %s AND `regdate` <= %s
ORDER BY `alias_id` desc 