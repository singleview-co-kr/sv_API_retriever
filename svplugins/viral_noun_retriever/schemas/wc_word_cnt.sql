CREATE TABLE `wc_word_cnt` (
  `log_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `referral` tinyint unsigned NOT NULL,
  `document_srl` bigint(11) NOT NULL,
  `module_srl` bigint(11) NOT NULL,
  `word_srl` bigint(11) NOT NULL,
  `cnt` TINYINT unsigned NOT NULL,
  `logdate` DATETIME NOT NULL,
  PRIMARY KEY (`log_srl`),
  KEY `idx_document_srl` (`document_srl`),
  KEY `idx_word_srl` (`word_srl`),
  KEY `idx_logdate` (`logdate`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;