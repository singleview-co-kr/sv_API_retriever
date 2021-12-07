CREATE TABLE `ga_transaction_daily_log` (
  `log_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `3rd_party_tr_id` varchar(100) NOT NULL,
  `ua` char(1) NOT NULL,
  `source` varchar(50) NOT NULL,
  `rst_type` varchar(5) NOT NULL DEFAULT '0',
  `media` varchar(10) NOT NULL,
  `brd` TINYINT NOT NULL DEFAULT '0',
  `campaign_1st` varchar(100) NOT NULL DEFAULT '0',
  `campaign_2nd` varchar(25) NOT NULL DEFAULT '0',
  `campaign_3rd` varchar(25) NOT NULL DEFAULT '0',
  `term` varchar(1000) NOT NULL DEFAULT '0',
  `revenue` INT(11) UNSIGNED NOT NULL DEFAULT 0,
  `logdate` DATE NOT NULL,
  `regdate` DATETIME NOT NULL DEFAULT NOW(),
  PRIMARY KEY (`log_srl`),
  KEY `idx_3rd_party_tr_id` (`3rd_party_tr_id`),
  KEY `idx_dt` (`logdate`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;