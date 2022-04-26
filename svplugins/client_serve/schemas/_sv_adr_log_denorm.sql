CREATE TABLE `sv_adr_log_denorm` (
  `document_srl` bigint(11) NOT NULL,
  `addr_full` varchar(100) NOT NULL,
  `logdate` DATE NOT NULL,
  PRIMARY KEY (`document_srl`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;