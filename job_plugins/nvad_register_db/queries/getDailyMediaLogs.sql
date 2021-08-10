SELECT `customer_id`, `campaign_id`, `ad_group_id`, `ad_keyword_id`, `ad_id`, `pc_mobile_type`, `impression`, `click`, `cost`, `sum_of_adrank`
FROM `nvad_stat_ad`
WHERE `date`=%s AND `customer_id` = %s