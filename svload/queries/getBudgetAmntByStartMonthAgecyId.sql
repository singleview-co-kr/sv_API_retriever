SELECT `id`, `acct_id`, `media_agency_id`, `memo`, `target_amnt_inc_vat`, `date_begin`, `date_end`
FROM `budget`
WHERE year(date_begin) = %s AND month(date_begin) = %s AND media_agency_id = %s