SELECT MAX(`logdate`) AS maxdate
FROM `nvad_assembled_daily_log`
WHERE `rst_type` = 'PS' AND `media` = 'display' AND `campaign_1st` = 'BRS' AND `customer_id` = %s