SELECT log_srl
FROM `nvad_assembled_daily_log`
WHERE `logdate`=%s AND `media` = 'display' AND `rst_type` = 'PS' AND `campaign_1st` = 'BRS' AND `customer_id` = %s