SELECT `log_srl`, `media_agency_name`, `media_ua`, `media_term`, `media_source`, `media_rst_type`, `media_media`, `media_brd`,
	`media_camp1st`, `media_camp2nd`, `media_camp3rd`, `media_raw_cost`, `media_agency_cost`, `media_cost_vat`, `media_imp`, `media_click`, `media_conv_cnt`, `media_conv_amnt`,
	`in_site_tot_session`, `in_site_tot_new`, `in_site_tot_bounce`, `in_site_tot_duration_sec`,`in_site_tot_pvs`,`in_site_trs`, `in_site_revenue`, `in_site_registrations`, `logdate`
FROM `compiled_ga_media_daily_log`
WHERE `logdate` >= %s AND `logdate` <= %s