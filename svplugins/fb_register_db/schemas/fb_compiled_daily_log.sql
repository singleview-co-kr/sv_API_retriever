CREATE TABLE `fb_compiled_daily_log` (
  `log_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `biz_id` BIGINT(20) NOT NULL,
  `ua` char(1) NOT NULL,
  `source` char(9) NOT NULL,
  `rst_type` varchar(5) NOT NULL,
  `media` varchar(10) NOT NULL,
  `brd` TINYINT NOT NULL DEFAULT 0,
  `campaign_1st` varchar(100) NOT NULL,
  `campaign_2nd` varchar(25) DEFAULT NULL,
  `campaign_3rd` varchar(25) DEFAULT NULL,
  `cost` bigint(11) DEFAULT 0,
  `reach` bigint(11) DEFAULT 0,
  `imp` bigint(11) DEFAULT 0,
  `click` MEDIUMINT(11) UNSIGNED DEFAULT 0,
  `unique_click` MEDIUMINT(11) UNSIGNED DEFAULT 0,
  `conv_cnt` MEDIUMINT(11) UNSIGNED DEFAULT 0,
  `conv_amnt` bigint(11) UNSIGNED DEFAULT 0,
  `logdate` DATE NOT NULL,
  `regdate` DATETIME DEFAULT NOW(),
  PRIMARY KEY (`log_srl`),
  KEY `idx_dt_media` (`logdate`,`media`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;