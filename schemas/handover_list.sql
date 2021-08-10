CREATE TABLE `handover_list` (
  `handover_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `dbs_job_srl` bigint(11) NOT NULL,
  `caller_dbo_job_srl` bigint(11) NOT NULL DEFAULT 0,
  `caller_plugin_name` varchar(255) NOT NULL,
  `caller_plugin_params` text,
  `is_done` char(1) NOT NULL DEFAULT 'N',
  `regdate` varchar(14) NOT NULL,
  PRIMARY KEY (`handover_srl`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;