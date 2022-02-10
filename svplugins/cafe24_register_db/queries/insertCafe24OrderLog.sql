INSERT INTO `cafe24_order_log` 
(`order_no_detail`, `item_title`, `item_option`, `item_code`,
 `addr_do`, `addr_si`, `addr_gu_gun`, `addr_dong_myun_eup`,
 `cancel_type`, `refund_amnt`, `settlement_amnt`, `qty`, `discount_amnt`, `coupon_title`, `coupon_discount_amnt`, 
 `purchaser_id`, `purchaser_dob`, `referral`, `ext_order_id`, `settle_date`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)