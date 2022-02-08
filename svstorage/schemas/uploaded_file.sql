CREATE TABLE `uploaded_file` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `owner_id` int(11) NOT NULL,
  `source_filename` varchar(100) NOT NULL,
  `file_ext` varchar(5) NOT NULL,
  `secured_filename` varchar(20) NOT NULL,
  `simple_desc` varchar(100) NULL,
  `status` smallint DEFAULT 0,
  `deleted` tinyint DEFAULT 0,
  `regdt` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE `unique_secured_filename_ext` (`secured_filename`, `file_ext`),
  KEY `idx_source_filename` (`source_filename`),
  KEY `idx_deleted` (`deleted`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin