SELECT `ua`, `source`, `rst_type`, `media`, `brd`, `campaign_1st`, `campaign_2nd`, `campaign_3rd`,
 `term`, `session`, `new`, `bounce`, `duration_sec`, `pvs`
FROM `ga_compiled_daily_log`
WHERE `logdate` = %s
ORDER BY `log_srl`