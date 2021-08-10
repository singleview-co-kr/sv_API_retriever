CREATE TABLE `nvad_master_ad_grp` (
  `ad_grp_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `customer_id` bigint(11) NOT NULL,
  `ad_group_id` varchar(32) NOT NULL,
  `campaign_id` varchar(32) NOT NULL,
  `ad_group_name` varchar(40) NOT NULL,
  `ad_group_biz_amount` bigint(11) DEFAULT NULL,
  `on_off` int(1) DEFAULT NULL,
  `using_contents_network_bid` int(1) DEFAULT NULL,
  `contents_network_bid` bigint(11) DEFAULT NULL,
  `pc_network_bidding_weight` bigint(11) DEFAULT NULL,
  `mobile_network_bidding_weight` bigint(11) DEFAULT NULL,
  `using_keyword_plus` int(1) DEFAULT NULL,
  `keyword_plus_bidding_weight` bigint(11) DEFAULT NULL,
  `business_channel_id_mobile` varchar(32) NOT NULL,
  `business_channel_id_pc` varchar(32) NOT NULL,
  `regTm` DATETIME NOT NULL,
  `delTm` DATETIME DEFAULT NULL,
  `regdate` DATETIME DEFAULT NOW(),
  PRIMARY KEY (`ad_grp_srl`),
  KEY `idx_ad_group_id` (`ad_group_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;