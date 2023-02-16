CREATE TABLE `ytsearch_log` (
  `log_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `morpheme_srl` bigint(11) unsigned NOT NULL,
  `channel_id` varchar(30) NOT NULL,
  `channel_title` varchar(10) NOT NULL,
  `video_id` varchar(20) NOT NULL,
  `title` varchar(100) NOT NULL,
  `description` varchar(150) NULL,
  `b_crawled` char(1) NOT NULL DEFAULT '0',
  `logdate` DATETIME,
  `regdate` DATETIME DEFAULT NOW(),
  PRIMARY KEY (`log_srl`),
  KEY `idx_morpheme_srl` (`morpheme_srl`),
  KEY `idx_logdate` (`logdate`),
  KEY `idx_regdate` (`regdate`),
  KEY `idx_morpheme_srl_video_id` (`morpheme_srl`, `video_id`),
  KEY `idx_b_crawled` (`b_crawled`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;