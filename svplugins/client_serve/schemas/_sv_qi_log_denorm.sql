CREATE TABLE `sv_qi_log_denorm` (
  `source_id` TINYINT(3) UNSIGNED NOT NULL,
  `keyword` char(50) NOT NULL,
  `qi_max` TINYINT unsigned NOT NULL,
  `qi_min` TINYINT unsigned NOT NULL,
  `logdate` DATETIME NOT NULL,
  KEY `idx_source_id_keyword` (`source_id`, `keyword`),
  KEY `idx_logdate` (`logdate`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;