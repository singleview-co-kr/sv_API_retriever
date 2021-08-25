SELECT `log_srl`, `customer_id`, `cost`, `imp`, `click`, `conv_cnt`, `conv_amnt`
FROM `aw_compiled_daily_log`
WHERE `logdate` = %s AND `source` = %s AND `media` = %s AND `term` = %s AND `campaign_1st` = %s AND `campaign_2nd` = %s AND `campaign_3rd` = %s AND `ua` = %s