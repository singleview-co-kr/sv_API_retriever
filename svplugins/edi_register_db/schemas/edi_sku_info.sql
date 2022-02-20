CREATE TABLE `edi_sku_info` (
  `id` smallint(5) unsigned NOT NULL AUTO_INCREMENT,
  `b_accept` char(1) DEFAULT 0,
  `mart_id` int(11) NOT NULL,
  `item_code` varchar(20) COLLATE utf8_bin NOT NULL,
  `bar_code` varchar(20) COLLATE utf8_bin NULL,
  `item_name` varchar(100) COLLATE utf8_bin NOT NULL,
  `margin_ratio` int(2) UNSIGNED NULL,
  `first_detect_logdate` date NOT NULL,
  `regdt` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE `unique_item_code` (`mart_id`, `item_code`),
  KEY `idx_item_name` (`item_name`),
  KEY `idx_b_accept` (`b_accept`),
  KEY `idx_mart_id` (`mart_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin