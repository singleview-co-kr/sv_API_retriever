CREATE TABLE `budget` (
  `id` mediumint(11) unsigned NOT NULL AUTO_INCREMENT,
  `owner_id` int(11) unsigned NOT NULL,
  `acct_id` tinyint(11) unsigned NOT NULL DEFAULT 0,
  `media_agency_id` bigint(11) DEFAULT 0,
  `alloc_yr` smallint(5) unsigned NOT NULL,
  `alloc_mo` tinyint(5) unsigned NOT NULL,
  `memo` varchar(400) NOT NULL,
  `target_amnt_inc_vat` INT(11) NOT NULL DEFAULT 0,
  `actual_amnt_inc_vat` INT(11) NOT NULL DEFAULT 0,
  `date_begin` DATE NOT NULL,
  `date_end` DATE NOT NULL,
  `closed` char(1) NOT NULL DEFAULT 0,
  `regdate` DATETIME DEFAULT NOW() NOT NULL,
   PRIMARY KEY (`id`),
   KEY `idx_period` (`date_begin`, `date_end`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin