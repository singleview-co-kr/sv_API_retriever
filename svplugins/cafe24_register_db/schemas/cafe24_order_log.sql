CREATE TABLE `cafe24_order_log` (
  `log_srl` bigint(11) NOT NULL AUTO_INCREMENT,
  `order_no_detail` varchar(25) NOT NULL,
  `item_title` varchar(50) NOT NULL,
  `item_option` varchar(20) DEFAULT NULL,
  `item_code` varchar(20) DEFAULT NULL,
  `addr_do` varchar(4) NOT NULL,
  `addr_si` varchar(8) NOT NULL,
  `addr_gu_gun` varchar(20) DEFAULT NULL,
  `addr_dong_myun_eup` varchar(20) DEFAULT NULL,
  `cancel_type` char(7) DEFAULT NULL,
  `refund_amnt` MEDIUMINT(11) UNSIGNED DEFAULT 0,
  `settlement_amnt` MEDIUMINT(11) UNSIGNED DEFAULT 0,
  `qty` TINYINT(5) UNSIGNED NOT NULL,
  `discount_amnt` MEDIUMINT(11) UNSIGNED DEFAULT 0,
  `coupon_title` varchar(25) DEFAULT NULL,
  `coupon_discount_amnt` MEDIUMINT(11) UNSIGNED DEFAULT 0,
  `purchaser_id` varchar(25) DEFAULT NULL,
  `purchaser_dob` DATE DEFAULT NULL,
  `referral` varchar(15) NOT NULL,
  `ext_order_id` varchar(25) DEFAULT NULL,
  `settle_date` DATE NOT NULL,
  `regdate` DATETIME DEFAULT NOW(),
  PRIMARY KEY (`log_srl`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;