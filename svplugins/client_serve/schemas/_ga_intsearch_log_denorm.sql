CREATE TABLE `ga_intsearch_log_denorm` (
  `word` varchar(50) NOT NULL,
  `ua` char(1) NOT NULL,
  `cnt` TINYINT unsigned NOT NULL,
  `logdate` DATE NOT NULL,
  KEY `idx_logdate` (`logdate`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;