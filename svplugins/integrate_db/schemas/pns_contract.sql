CREATE TABLE `pns_contract` (
  `contract_id` bigint(11) NOT NULL AUTO_INCREMENT,
  `source_id` SMALLINT(3) UNSIGNED NOT NULL,
  `contract_type` SMALLINT(3) UNSIGNED NOT NULL DEFAULT 0,
  `media_term` varchar(100) NOT NULL,
  `nick_name` varchar(25) DEFAULT NULL,
  `cost_incl_vat` bigint(11) DEFAULT 0,
  `agency_rate_percent` varchar(4) NOT NULL,
  `contract_date_begin` DATE NOT NULL,
  `contract_date_end` DATE NOT NULL,
  `regdate` DATETIME DEFAULT NOW(),
   PRIMARY KEY (`contract_id`),
   KEY `idx_contract_period` (`contract_date_begin`, `contract_date_end`),
   KEY `idx_contract_date_end` (`contract_date_end`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;