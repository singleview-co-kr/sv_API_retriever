SELECT `id`, `mart_id`, `item_name`, `first_detect_logdate`
FROM `edi_sku_info`
WHERE `b_accept` = %s