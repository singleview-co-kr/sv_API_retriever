SELECT handover_srl, caller_dbo_job_srl, caller_plugin_name, caller_plugin_params
FROM `handover_list`
WHERE handover_srl=%s