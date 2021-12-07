INSERT INTO `ga_itemperf_daily_log` 
(`item_srl`, `ua`, `imp_list`, `click_list`, `imp_detail`, `freq_cart`, `qty_cart`,
 `qty_cart_remove`, `amnt_pur`, `freq_pur`, `freq_cko`, `qty_cko`, `logdate`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)