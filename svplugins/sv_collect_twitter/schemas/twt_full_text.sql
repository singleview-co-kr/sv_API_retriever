CREATE TABLE `twt_full_text` (
  `full_text_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `full_text` varchar(280) NOT NULL,
  PRIMARY KEY (`full_text_srl`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;