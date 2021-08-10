SELECT `log_srl`, `customer_id`, `ua`, `term`, `rst_type`, `media`, `brd`, `campaign_1st`, `campaign_2nd`, `campaign_3rd`, `cost_inc_vat`, `imp`, `click`, `conv_cnt_direct`, `conv_amnt_direct`, `conv_cnt_indirect`, `conv_amnt_indirect`
FROM `kko_compiled_daily_log`
WHERE `logdate` = %s