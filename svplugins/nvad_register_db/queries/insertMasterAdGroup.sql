INSERT INTO `nvad_master_ad_grp` 
(`customer_id`, `ad_group_id`, `campaign_id`, `ad_group_name`, `ad_group_biz_amount`,`on_off`, `using_contents_network_bid`,
`contents_network_bid`,`pc_network_bidding_weight`, `mobile_network_bidding_weight`,`using_keyword_plus`,`keyword_plus_bidding_weight`,
`business_channel_id_mobile`, `business_channel_id_pc`, `regTm`, `delTm`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s)