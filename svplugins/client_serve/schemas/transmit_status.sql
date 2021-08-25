CREATE TABLE `transmit_status` (
  `status_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `status` char(1) NOT NULL,
  `client_ip` char(25) NOT NULL,
  `regdate` DATETIME DEFAULT NOW(),
  PRIMARY KEY (`status_srl`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;