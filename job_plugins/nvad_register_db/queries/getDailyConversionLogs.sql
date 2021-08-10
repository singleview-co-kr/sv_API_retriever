SELECT `campaign_id`, `ad_group_id`, `ad_keyword_id`, `ad_id`, `pc_mobile_type`, `conversion_count`, `sales_by_conversion`
FROM `nvad_stat_ad_conversion`
WHERE `date`=%s AND `customer_id` = %s