SELECT `log_srl`, `biz_id`, `cost`, `imp`, `click`, `conv_cnt`, `conv_amnt`
FROM `fb_compiled_daily_log`
WHERE `logdate` = %s AND `media` = %s AND `source` = %s AND `campaign_1st` = %s AND `campaign_2nd` = %s AND `campaign_3rd` = %s AND `ua` = %s