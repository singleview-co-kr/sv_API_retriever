CREATE TABLE `wc_document_log` (
  `log_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `referral` tinyint unsigned NOT NULL,
  `document_srl` bigint(11) NOT NULL,
  `title` varchar(250) NOT NULL,
  `content` LongText NOT NULL,
  `b_proc` char(1) NOT NULL DEFAULT '0',
  `logdate` DATETIME NOT NULL,
  `regdate` DATETIME DEFAULT NOW(),
  PRIMARY KEY (`log_srl`),
  KEY `idx_logdate` (`logdate`),
  KEY `idx_document_srl` (`document_srl`),
  KEY `idx_b_proc` (`b_proc`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;