CREATE TABLE `nvad_master_ad` (
  `ad_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `customer_id` bigint(11) NOT NULL,
  `ad_group_id` varchar(32) NOT NULL,
  `ad_id` varchar(32) NOT NULL,
  `ad_creative_inspect_status` int(2) DEFAULT NULL,
  `subject` varchar(266) NOT NULL,
  `description` varchar(266) NOT NULL,
  `landing_url_pc` varchar(1024) NOT NULL,
  `landing_url_mobile` varchar(1024) NOT NULL,
  `on_off` int(1) DEFAULT NULL,
  `regTm` DATETIME NOT NULL,
  `delTm` DATETIME DEFAULT NULL,
  `regdate` DATETIME DEFAULT NOW(),
  PRIMARY KEY (`ad_srl`),
  KEY `idx_reg_tm` (`regTm`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;