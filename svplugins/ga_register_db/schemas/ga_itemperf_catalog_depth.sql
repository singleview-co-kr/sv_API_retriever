CREATE TABLE `ga_itemperf_catalog_depth` (
  `catalog_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `item_srl` bigint(11) NOT NULL,
  `cat_depth` tinyint(2) NOT NULL,
  `cat_title` varchar(50) NOT NULL,
  `regdate` DATETIME DEFAULT NOW(),
  PRIMARY KEY (`catalog_srl`),
  KEY `idx_item_srl_cat_depth` (`item_srl`, `cat_depth`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;