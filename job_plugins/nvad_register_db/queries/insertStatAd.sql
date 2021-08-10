INSERT INTO `nvad_stat_ad` 
(`date`,`customer_id`,`campaign_id`, `ad_group_id`, `ad_keyword_id`,`ad_id`, `business_channel_id`,`mediacode`,`pc_mobile_type`,`impression`,`click`,`cost`, `sum_of_adrank`)
VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s,%s,%s,%s)