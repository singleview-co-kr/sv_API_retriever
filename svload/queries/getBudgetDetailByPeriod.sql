SELECT `id`, `acct_id`, `media_agency_id`, `alloc_yr`, `alloc_mo`, `memo`, `target_amnt_inc_vat`, `ua`, `date_begin`, `date_end`, `regdate`
FROM `budget`
WHERE `date_begin` >= %s AND `date_end` <= %s AND `deleted` = 0