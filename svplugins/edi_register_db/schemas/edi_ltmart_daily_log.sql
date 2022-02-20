CREATE TABLE `edi_ltmart_daily_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `item_id` smallint(5) unsigned NOT NULL,
  `branch_id` int(11) NOT NULL,
  `qty` smallint DEFAULT 0,
  `original_amnt` int DEFAULT 0,
  `amnt` int DEFAULT 0,
  `logdate_since` date NOT NULL,
  `logdate` date NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE `unique_mart_item_branch_logdate` (`item_id`,`branch_id`,`logdate_since`,`logdate`),
  KEY `idx_logdate` (`logdate`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin