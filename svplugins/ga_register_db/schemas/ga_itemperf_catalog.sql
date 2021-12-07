CREATE TABLE `ga_itemperf_catalog` (
  `item_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `item_title` varchar(100) NOT NULL,
  `b_ignore` char(1) NOT NULL DEFAULT '0',
  `first_detect_logdate` date NOT NULL,
  `regdate` DATETIME DEFAULT NOW(),
  PRIMARY KEY (`item_srl`),
  KEY `idx_item_title` (`item_title`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;