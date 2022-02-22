SELECT `id`, `acct_id`, `alloc_yr`, `alloc_mo`, `memo`, `target_amnt_inc_vat`, `actual_amnt_inc_vat`, `date_begin`, `date_end`, `regdate`
FROM `budget`
WHERE `date_begin` >= %s AND `date_end` <= %s