SELECT handover_srl, dbs_job_srl, caller_plugin_name, caller_plugin_params, regdate
FROM `handover_list`
WHERE is_done='N'
ORDER BY handover_srl ASC