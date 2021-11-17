CREATE TABLE `twt_user` (
  `user_id` bigint(11) NOT NULL,
  `follower_cnt` int(11) unsigned NOT NULL,
  `following_cnt` int(11) unsigned NOT NULL,
  `favourites_cnt` int(11) unsigned NOT NULL,
  `friend_cnt` int(11) unsigned NOT NULL,
  PRIMARY KEY (`user_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;