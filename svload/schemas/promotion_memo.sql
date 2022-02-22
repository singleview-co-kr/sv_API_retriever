CREATE TABLE `promotion_memo` (
  `id` mediumint(11) unsigned NOT NULL AUTO_INCREMENT,
  `owner_id` int(11) unsigned NOT NULL,
  `item_id` smallint(5) unsigned NOT NULL DEFAULT 0,
  `branch_id` int(11) unsigned NOT NULL DEFAULT 0,
  `memo` varchar(500) NOT NULL,
  `disc_rate` TINYINT(3) NOT NULL DEFAULT 0,
  `date_begin` DATE NOT NULL,
  `date_end` DATE NOT NULL,
  `acted` char(1) NOT NULL DEFAULT 0,
  `regdate` DATETIME DEFAULT NOW() NOT NULL,
   PRIMARY KEY (`id`),
   KEY `idx_item_id` (`item_id`),
   KEY `idx_branch_id` (`branch_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin