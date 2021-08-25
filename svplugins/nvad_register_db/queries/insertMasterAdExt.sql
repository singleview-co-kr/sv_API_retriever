INSERT INTO `nvad_master_ad_extension` 
(`customer_id`, `ad_extension_id`,`type`,`owner_id`,`biz_channel_id_pc`,`biz_channel_id_mobile`,
`time_targeting_monday`,`time_targeting_tuesday`,`time_targeting_wednesday`,`time_targeting_thursday`,`time_targeting_friday`,`time_targeting_saturday`,`time_targeting_sunday`,
`on_off`,`ad_extension_inspect_status`,`regTm`, `delTm`)
VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s,%s, %s,%s, %s, %s, %s,%s)