SELECT `ua`, `source`, `rst_type`, `media`, `brd`, `campaign_1st`, `campaign_2nd`, `campaign_3rd`, `term`, `session`,
  `new_per`, `bounce_per`, `duration_sec`, `pvs`, `transactions`, `revenue`, `registrations`
FROM `ga_compiled_daily_log`
WHERE `logdate` = %s
ORDER BY `log_srl`