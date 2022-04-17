CREATE TABLE `twt_morpheme` (
  `morpheme_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `morpheme` varchar(100) NOT NULL,
  `b_proc` char(1) NOT NULL DEFAULT '1',
  `regdate` DATETIME DEFAULT NOW(),
  PRIMARY KEY (`morpheme_srl`),
  KEY `idx_morpheme` (`morpheme`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;