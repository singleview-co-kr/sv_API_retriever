CREATE TABLE `nvad_master_campaign` (
  `campaign_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `customer_id` bigint(11) NOT NULL,
  `campaign_id` varchar(32) NOT NULL,
  `campaign_name` varchar(32) NOT NULL,
  `campaign_type` int(1) NOT NULL,
  `delivery_method` int(1) NOT NULL,
  `using_period` int(1) NOT NULL,
  `period_start_dt` DATETIME DEFAULT NULL,
  `period_end_dt` DATETIME DEFAULT NULL,
  `regTm` DATETIME NOT NULL,
  `delTm` DATETIME DEFAULT NULL,
  `regdate` DATETIME DEFAULT NOW(),
  PRIMARY KEY (`campaign_srl`),
  KEY `idx_campaign_id` (`campaign_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;