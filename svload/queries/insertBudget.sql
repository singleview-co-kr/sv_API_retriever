INSERT INTO `budget`
(`owner_id`, `acct_id`, `media_agency_id`, `alloc_yr`, `alloc_mo`, `memo`, `ua`, `target_amnt_inc_vat`, `date_begin`, `date_end`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)