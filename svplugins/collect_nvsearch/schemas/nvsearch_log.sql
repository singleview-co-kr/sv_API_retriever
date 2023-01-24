CREATE TABLE `nvsearch_log` (
  `log_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `morpheme_srl` bigint(11) unsigned NOT NULL,
  `media_id` tinyint(2) unsigned NOT NULL,
  `title` varchar(50) NOT NULL,
  `link` varchar(100) NOT NULL,
  `description` varchar(150) NULL,
  `extra_vars` varchar(1000) NULL,
  `full_text_srl` bigint(11) unsigned NULL,
  `b_crawled` char(1) NOT NULL DEFAULT '0',
  `logdate` DATETIME,
  `regdate` DATETIME DEFAULT NOW(),
  PRIMARY KEY (`log_srl`),
  KEY `idx_morpheme_srl` (`morpheme_srl`),
  KEY `idx_pubdate` (`logdate`),
  KEY `idx_morpheme_srl_media_id_link` (`morpheme_srl`, `media_id`, `link`),
  KEY `idx_b_crawled` (`b_crawled`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;