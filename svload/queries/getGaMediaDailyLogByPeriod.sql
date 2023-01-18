SELECT `media_ua`, `media_rst_type`, `media_source`, `media_media`, `media_raw_cost`, `media_agency_cost`, `media_cost_vat`,
`media_imp`, `media_click`, `media_term`, `media_brd`, `in_site_tot_session`, `in_site_tot_bounce`, `in_site_tot_duration_sec`,
`in_site_tot_pvs`, `in_site_tot_new`, `in_site_revenue`, `in_site_trs`, `media_camp1st`, `media_camp2nd`, `media_camp3rd`, `logdate`
FROM `compiled_ga_media_daily_log`
WHERE `logdate` >= %s AND `logdate` <= %s