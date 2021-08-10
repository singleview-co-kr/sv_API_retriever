CREATE TABLE `svnvcrawl_job` (
  `job_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `account_srl` bigint(11) NOT NULL DEFAULT '0',
  `job_title` varchar(255) NOT NULL,
  `start_date` varchar(10) DEFAULT NULL,
  `end_date` varchar(10) DEFAULT NULL,
  `plugin_name` varchar(255) NOT NULL,
  `plugin_params` text,
  `job_trigger_type` varchar(15) NOT NULL DEFAULT 'none',
  `trigger_params` text,
  `is_active` char(1) NOT NULL DEFAULT 'N',
  `is_deleted` char(1) NOT NULL DEFAULT 'N',
  `regdate` varchar(14) NOT NULL,
  `modification_date` varchar(14) NOT NULL,
  `application_date` varchar(14) NOT NULL,
  PRIMARY KEY (`job_srl`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

