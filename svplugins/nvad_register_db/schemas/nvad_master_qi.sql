CREATE TABLE `nvad_master_qi` (
  `qi_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `customer_id` bigint(11) NOT NULL,
  `ad_group_id` varchar(32) NOT NULL,
  `ad_keyword_id` varchar(32) NOT NULL,
  `ad_keyword` varchar(320) NOT NULL,
  `quality_index` int(1) DEFAULT NULL,
  `check_date` DATETIME NOT NULL,
  `regdate` DATETIME DEFAULT NOW(),
  PRIMARY KEY (`qi_srl`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;