UPDATE `budget`
SET `acct_id` = %s, `media_agency_id` = %s, `alloc_yr` = %s, `alloc_mo` = %s, `memo` = %s, `ua` = %s, `target_amnt_inc_vat` = %s, `date_begin` = %s, `date_end` = %s, `deleted` = %s
WHERE `id` = %s