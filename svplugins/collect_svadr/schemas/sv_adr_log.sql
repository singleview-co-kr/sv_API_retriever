CREATE TABLE `sv_adr_log` (
  `log_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `document_srl` bigint(11) NOT NULL,
  `module_srl` bigint(11) NOT NULL,
  `postcode` char(6) NULL,
  `addr_do` varchar(4) NOT NULL,
  `addr_si` varchar(8) NOT NULL,
  `addr_gu_gun` varchar(20) DEFAULT NULL,
  `addr_dong_myun_eup` varchar(20) DEFAULT NULL,
  `addr` varchar(200) DEFAULT NULL,
  `logdate` DATE NOT NULL,
  `regdate` DATETIME DEFAULT NOW(),
  PRIMARY KEY (`log_srl`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;