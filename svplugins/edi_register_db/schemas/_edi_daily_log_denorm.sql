CREATE TABLE `_edi_daily_log_denorm` (
  `mart_name` varchar(10) COLLATE utf8_bin NOT NULL,
  `mart_type` varchar(10) COLLATE utf8_bin NOT NULL,
  `branch_name` varchar(50) COLLATE utf8_bin NOT NULL,
  `item_name` varchar(50) COLLATE utf8_bin NOT NULL,
  `qty` smallint DEFAULT 0,
  `amnt` int DEFAULT 0,
  `do` varchar(10) COLLATE utf8_bin NOT NULL,
  `si` varchar(20) COLLATE utf8_bin NOT NULL,
  `gu` varchar(20) COLLATE utf8_bin NULL,
  `dong` varchar(20) COLLATE utf8_bin NULL,
  `lati_longi` varchar(30) COLLATE utf8_bin NULL,
  `logdate` date NOT NULL,
  KEY `idx_logdate` (`logdate`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin