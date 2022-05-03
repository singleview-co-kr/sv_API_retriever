UPDATE `svdaemon_job`
SET dt_applied=now()
WHERE id=%s