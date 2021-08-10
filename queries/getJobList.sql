SELECT job_srl, job_title, plugin_name, plugin_params, job_trigger_type, trigger_params, start_date, end_date, modification_date, application_date
FROM `svnvcrawl_job`
WHERE is_active=%s