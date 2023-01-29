CREATE TABLE `morphemes` (
  `morpheme_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `morpheme` varchar(100) NOT NULL,
  `b_toggle` char(1) NOT NULL DEFAULT '1',
  `regdate` DATETIME DEFAULT NOW(),
  PRIMARY KEY (`morpheme_srl`),
  UNIQUE `uniq_morpheme` (`morpheme`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;