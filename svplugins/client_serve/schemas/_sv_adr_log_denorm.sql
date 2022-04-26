CREATE TABLE `sv_adr_log_denorm` (
  `document_srl` bigint(11) NOT NULL,
  `addr_full` varchar(150) NOT NULL,
  `addr_do` varchar(4) NOT NULL,
  `addr_si` varchar(8) NOT NULL,
  `addr_gu_gun` varchar(20) DEFAULT NULL,
  `addr_dong_myun_eup` varchar(20) DEFAULT NULL,
  `logdate` DATE NOT NULL,
  PRIMARY KEY (`document_srl`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;