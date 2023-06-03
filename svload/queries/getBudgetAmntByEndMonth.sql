SELECT `id`, `acct_id`, `media_agency_id`, `memo`, `target_amnt_inc_vat`, `date_begin`, `date_end`
FROM `budget`
WHERE year(date_end) = %s AND month(date_end) = %s AND `deleted` = 0