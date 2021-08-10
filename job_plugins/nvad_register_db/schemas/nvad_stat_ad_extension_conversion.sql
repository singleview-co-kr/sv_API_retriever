CREATE TABLE `nvad_stat_ad_extension_conversion` (
  `ad_extension_conversion_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `date` DATE NOT NULL,
  `customer_id` bigint(11) NOT NULL,
  `campaign_id` varchar(32) NOT NULL,
  `ad_group_id` varchar(32) NOT NULL,
  `ad_keyword_id` varchar(32) NOT NULL,
  `ad_id` varchar(32) NOT NULL,
  `ad_extension_id` varchar(32) NOT NULL,
  `ad_extension_business_channel_id` varchar(32) NOT NULL,
  `mediacode` varchar(32) NOT NULL,
  `pc_mobile_type` char(1) DEFAULT NULL,
  `conversion_method` int(1) DEFAULT NULL,
  `conversion_type` int(1) DEFAULT NULL,
  `conversion_count` bigint(11) NOT NULL,
  `sales_by_conversion` bigint(11) NOT NULL,
  PRIMARY KEY (`ad_extension_conversion_srl`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;