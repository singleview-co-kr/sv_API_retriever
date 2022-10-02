SELECT `acct_id`, `media_agency_id`, `alloc_yr`, `alloc_mo`, `memo`, `target_amnt_inc_vat`, `date_begin`, `date_end`, `closed`
FROM `budget`
WHERE `id` = %s