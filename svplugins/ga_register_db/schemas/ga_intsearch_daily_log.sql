CREATE TABLE `ga_intsearch_daily_log` (
  `log_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `word_srl` bigint(11) NOT NULL,
  `ua` char(1) NOT NULL,
  `cnt` TINYINT unsigned NOT NULL,
  `logdate` DATE NOT NULL,
  PRIMARY KEY (`log_srl`),
  KEY `idx_word_srl` (`word_srl`),
  KEY `idx_logdate` (`logdate`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;