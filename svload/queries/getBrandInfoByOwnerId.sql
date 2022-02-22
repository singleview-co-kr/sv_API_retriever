SELECT `id`, `owner_id`, `brand_name`, `ga_view_id`
FROM `edi_brand_info`
WHERE `owner_id` = %s AND `ga_view_id` IS NOT NULL