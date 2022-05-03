SELECT `id`, `sv_acct_id`,`sv_brand_id`, `s_job_title`, `s_plugin_name`, `s_plugin_params`, `s_trigger_type`, `s_trigger_params`, `date_start`, `date_end`, `dt_mod`, `dt_applied` 
FROM `svdaemon_job`
WHERE is_active=%s