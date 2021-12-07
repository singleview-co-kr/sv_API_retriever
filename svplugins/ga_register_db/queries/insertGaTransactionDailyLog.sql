INSERT INTO `ga_transaction_daily_log` 
(`3rd_party_tr_id`, `ua`, `source`, `rst_type`, `media`, `brd`, `campaign_1st`, `campaign_2nd`, `campaign_3rd`,
`term`, `revenue`, `logdate`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)