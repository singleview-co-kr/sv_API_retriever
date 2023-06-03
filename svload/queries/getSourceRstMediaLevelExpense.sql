SELECT SUM(`media_raw_cost`), SUM(`media_agency_cost`), SUM(`media_cost_vat`)
FROM `compiled_ga_media_daily_log`
WHERE `media_rst_type` = %s AND `media_source` = %s AND `media_media` = %s AND `logdate` >= %s AND `logdate` <= %s