SELECT `log_srl`, `customer_id`, `cost`, `imp`, `click`, `conv_cnt`, `conv_amnt`
FROM `nvad_assembled_daily_log`
WHERE `logdate` = %s AND `media` = %s AND binary(`term`) = %s AND `campaign_1st` = %s AND `campaign_2nd` = %s AND `ua` = %s