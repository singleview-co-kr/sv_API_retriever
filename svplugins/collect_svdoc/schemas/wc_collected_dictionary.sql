CREATE TABLE `wc_collected_dictionary` (
  `word_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `word` char(50) NOT NULL,
  `b_ignore` char(1) NOT NULL DEFAULT '0',
  `regdate` DATETIME DEFAULT NOW(),
  PRIMARY KEY (`word_srl`),
  KEY `idx_word` (`word`),
  KEY `idx_regdate` (`regdate`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;