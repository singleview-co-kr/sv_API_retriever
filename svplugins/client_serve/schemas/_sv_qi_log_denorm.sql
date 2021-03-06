CREATE TABLE `sv_qi_log_denorm` (
  `source` varchar(10) NOT NULL,
  `keyword` char(50) NOT NULL,
  `campaign_name` varchar(80) NOT NULL,
  `qi` TINYINT unsigned NOT NULL,
  `logdate` DATETIME NOT NULL,
  KEY `idx_source_keyword` (`source`, `keyword`),
  KEY `idx_logdate` (`logdate`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;