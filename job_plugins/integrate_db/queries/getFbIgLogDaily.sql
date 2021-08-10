SELECT `log_srl`, `biz_id`, `ua`, `source`, `rst_type`, `media`, `brd`, `campaign_1st`, `campaign_2nd`, `campaign_3rd`, `cost`, `imp`, `click`, `conv_cnt`, `conv_amnt`
FROM `fb_compiled_daily_log`
WHERE `logdate` = %s