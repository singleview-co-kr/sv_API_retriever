CREATE TABLE `ga_itemperf_daily_log` (
  `log_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `item_srl` bigint(11) NOT NULL,
  `ua` char(1) NOT NULL,
  `imp_list` smallint DEFAULT 0,
  `click_list` smallint DEFAULT 0,
  `imp_detail` smallint DEFAULT 0,
  `freq_cart` smallint DEFAULT 0,
  `qty_cart` smallint DEFAULT 0,
  `qty_cart_remove` smallint DEFAULT 0,
  `amnt_pur` int DEFAULT 0,
  `freq_pur` smallint DEFAULT 0,
  `freq_cko` smallint DEFAULT 0,
  `qty_cko` smallint DEFAULT 0,
  `logdate` DATE NOT NULL,
  PRIMARY KEY (`log_srl`),
  KEY `idx_item_srl` (`item_srl`),
  KEY `idx_logdate` (`logdate`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;