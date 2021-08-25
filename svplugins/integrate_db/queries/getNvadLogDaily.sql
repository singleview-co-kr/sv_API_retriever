SELECT `log_srl`, `customer_id`, `ua`, `term`, `rst_type`, `media`, `brd`, `campaign_1st`, `campaign_2nd`, `campaign_3rd`, `cost`, `imp`, `click`, `conv_cnt`, `conv_amnt`
FROM `nvad_assembled_daily_log`
WHERE `logdate` = %s