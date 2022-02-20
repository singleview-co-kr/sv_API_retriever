SELECT `id`, `mart_id`, `item_code`
FROM `edi_sku_info`
WHERE `b_accept` = %s
ORDER BY `item_code` DESC