CREATE TABLE `scraper_site_log` (
  `log_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `url` varchar(150) NULL,
  `finger_print` char(33) NULL,
  `content` TEXT NULL,
  `status_code` smallint,
  `regdate` DATETIME DEFAULT NOW(),
  `scrape_date` DATE NULL,
  `document_date` DATE NULL,
  PRIMARY KEY (`log_srl`),
  UNIQUE `unique_url` (`url`),
  KEY `idx_finger_print` (`finger_print`),
  KEY `idx_scrape_date` (`scrape_date`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;