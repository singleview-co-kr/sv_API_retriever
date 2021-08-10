CREATE TABLE `nvad_master_bizch` (
  `bizch_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `customer_id` bigint(11) NOT NULL,
  `name` varchar(32) NOT NULL,
  `business_channel_id` varchar(32) NOT NULL,
  `business_channel_type` int(1) NOT NULL,
  `site_url` varchar(255) NOT NULL,
  `phone_number` varchar(20) NULL,
  `Address` text NULL,
  `naver_booking_service` text NULL,
  `naver_talk_service` varchar(100) NULL,
  `pc_inspect_status` int(1) NOT NULL,
  `mobile_inspect_status` int(1) NOT NULL,
  `regTm` DATETIME NOT NULL,
  `delTm` DATETIME DEFAULT NULL,
  `regdate` DATETIME DEFAULT NOW(),
  PRIMARY KEY (`bizch_srl`),
  KEY `idx_reg_tm` (`regTm`)
) ENGINE=MyISAM  DEFAULT CHARSET=utf8;