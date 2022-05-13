CREATE TABLE `ga_itemperf_log_denorm` (
  `item_title` varchar(100) NOT NULL,
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
  KEY `idx_item_title` (`item_title`),
  KEY `idx_logdate` (`logdate`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;