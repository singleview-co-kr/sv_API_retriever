SELECT MIN(CAST(statdate AS CHAR)) as min_date
FROM svnvcrawl_stat_report_status
WHERE advertiser_id=%s AND report_type =%s