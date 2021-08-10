CREATE TABLE `nvad_master_campaign_budget` (
  `campaign_budget_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `customer_id` bigint(11) NOT NULL,
  `campaign_id` varchar(32) NOT NULL,
  `using_daily_budget` int(1) DEFAULT NULL,
  `daily_budget` bigint(11) DEFAULT NULL,
  `regTm` DATETIME NOT NULL,
  `delTm` DATETIME DEFAULT NULL,
  `regdate` DATETIME DEFAULT NOW(),
  PRIMARY KEY (`campaign_budget_srl`),
  KEY `idx_reg_tm` (`regTm`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;