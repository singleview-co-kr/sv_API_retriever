SELECT `campaign_name`, `campaign_type`
FROM `nvad_master_campaign`
WHERE `campaign_id` = %s AND DATE(DATE_ADD(`regTm`, INTERVAL -1 DAY)) <= %s
ORDER BY `campaign_srl` DESC