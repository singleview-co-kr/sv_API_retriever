CREATE TABLE `nvad_master_keyword` (
  `keyword_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `customer_id` bigint(11) NOT NULL,
  `ad_group_id` varchar(32) NOT NULL,
  `ad_keyword_id` varchar(32) NOT NULL,
  `ad_keyword` varchar(1000) NOT NULL,
  `ad_keyword_bid_amount` bigint(11) DEFAULT NULL,
  `landing_url_pc` text NOT NULL,
  `landing_url_mobile` text NOT NULL,
  `on_off` int(1) DEFAULT NULL,
  `ad_keyword_inspect_status` int(2) DEFAULT NULL,
  `using_ad_group_bid_amount` int(1) DEFAULT NULL,
  `regTm` DATETIME NOT NULL,
  `delTm` DATETIME DEFAULT NULL,
  `regdate` DATETIME DEFAULT NOW(),
  PRIMARY KEY (`keyword_srl`),
  KEY `idx_ad_keyword_id` (`ad_keyword_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;