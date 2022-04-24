CREATE TABLE `wc_word_cnt_denorm` (
  `word` char(50) NOT NULL,
  `module_srl` bigint(11) NOT NULL,
  `cnt` TINYINT unsigned NOT NULL,
  `logdate` DATETIME NOT NULL,
  KEY `idx_module_srl` (`module_srl`),
  KEY `idx_logdate` (`logdate`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;