CREATE TABLE `campaign_name_alias` (
  `alias_id` bigint(11) NOT NULL AUTO_INCREMENT,
  `source_id` TINYINT(3) UNSIGNED DEFAULT 0,
  `media_campaign_title` varchar(100) NOT NULL,
  `search_rst_id` TINYINT(3) UNSIGNED DEFAULT NULL,
  `medium_id` TINYINT(3) UNSIGNED DEFAULT NULL,
  `sv_lvl_1` varchar(20) DEFAULT '00',
  `sv_lvl_2` varchar(20) DEFAULT '00',
  `sv_lvl_3` varchar(20) DEFAULT '00',
  `sv_lvl_ext` varchar(50) DEFAULT NULL,
  `regdate` DATETIME DEFAULT NOW(),
   PRIMARY KEY (`alias_id`),
   KEY `idx_media_campaign_title` (`media_campaign_title`),
   UNIQUE KEY `uniq_source_id_media_campaign_title` (`source_id`, `media_campaign_title`),
   UNIQUE KEY `uniq_conventional_campaign_title` (`source_id`, `search_rst_id`, `medium_id`, `sv_lvl_1`, `sv_lvl_2`, `sv_lvl_3`, `sv_lvl_ext`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;