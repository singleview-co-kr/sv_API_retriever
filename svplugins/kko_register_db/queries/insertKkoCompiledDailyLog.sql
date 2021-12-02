INSERT INTO `kko_compiled_daily_log` 
(`customer_id`, `ua`, `source`, `rst_type`, `media`, `brd`, `campaign_1st`, `campaign_2nd`, `campaign_3rd`, `term`,
 `cost_inc_vat`, `imp`, `click`, `conv_cnt_direct`, `conv_amnt_direct`, `conv_cnt_indirect`, `conv_amnt_indirect`, `logdate`)
VALUES (%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)