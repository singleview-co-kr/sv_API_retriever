CREATE TABLE `wc_word_cnt_denorm` (
  `log_srl` bigint(11) NOT NULL,
  `word` char(50) NOT NULL,
  `cnt` TINYINT unsigned NOT NULL,
  `logdate` DATETIME NOT NULL,
  PRIMARY KEY (`log_srl`),
  KEY `idx_logdate` (`logdate`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;