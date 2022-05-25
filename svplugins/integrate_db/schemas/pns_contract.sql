CREATE TABLE `pns_contract` (
  `contract_id` bigint(11) NOT NULL AUTO_INCREMENT,
  `source_id` TINYINT(3) UNSIGNED NOT NULL,
  `contract_type` TINYINT(3) UNSIGNED NOT NULL DEFAULT 0,
  `media_term` varchar(100) NOT NULL,
  `contractor_id` varchar(25) DEFAULT NULL,
  `cost_incl_vat` bigint(11) DEFAULT 0,
  `agency_rate_percent` varchar(4) NOT NULL,
  `execute_date_begin` DATE NOT NULL,
  `execute_date_end` DATE NOT NULL,
  `regdate` DATETIME DEFAULT NOW(),
   PRIMARY KEY (`contract_id`),
   KEY `idx_source_id_execute_period` (`source_id`, `execute_date_begin`, `execute_date_end`),
   KEY `idx_execute_date_end` (`execute_date_end`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;