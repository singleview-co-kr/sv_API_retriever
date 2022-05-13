SELECT `item_srl`, `ua`, `imp_list`, `click_list`, `imp_detail`, `freq_cart`, `qty_cart`, `qty_cart_remove`, `amnt_pur`, `freq_pur`, `freq_cko`, `qty_cko`, `logdate`
FROM `ga_itemperf_daily_log`
WHERE `logdate` >= %s