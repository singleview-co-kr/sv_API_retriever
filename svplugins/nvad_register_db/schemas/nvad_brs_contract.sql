CREATE TABLE `nvad_brs_contract` (
  `contract_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `contract_id` varchar(30) NOT NULL,
  `contract_status` varchar(7) NOT NULL,
  `contract_regdate` DATE NOT NULL,
  `contract_name` varchar(100) NOT NULL,
  `conntected_ad_group` varchar(100) NOT NULL,
  `template_name` varchar(100) NOT NULL,
  `available_queries` int(8) unsigned NOT NULL,
  `contract_date_begin` DATE NOT NULL,
  `contract_date_end` DATE NOT NULL,
  `contract_amnt` int(8) unsigned NOT NULL,
  `refund_amnt` int(8) unsigned NOT NULL,
  `ua` char(1) NOT NULL,
  `regdate` DATETIME DEFAULT NOW(),
  PRIMARY KEY (`contract_srl`),
  UNIQUE KEY (`contract_id`),
  KEY `idx_contract_period` (`contract_date_begin`, `contract_date_end`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;