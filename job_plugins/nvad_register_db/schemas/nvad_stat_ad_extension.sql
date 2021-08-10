CREATE TABLE `nvad_stat_ad_extension` (
  `ad_extension_srl` bigint(11) NOT NULL AUTO_INCREMENT,
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
  `impression` bigint(11) NOT NULL,
  `click` bigint(11) NOT NULL,
  `cost` bigint(11) NOT NULL,
  `sum_of_adrank` bigint(11) NOT NULL,
  PRIMARY KEY (`ad_extension_srl`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;