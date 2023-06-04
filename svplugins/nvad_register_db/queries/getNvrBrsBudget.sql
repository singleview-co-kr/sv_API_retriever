SELECT `date_begin`, `date_end`, `target_amnt_inc_vat`, `ua`
FROM `budget`
WHERE `date_begin` <= %s AND `date_end` >= %s AND `acct_id` = %s
ORDER BY `id` ASC