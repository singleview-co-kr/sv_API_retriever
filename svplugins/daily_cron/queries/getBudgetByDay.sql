SELECT `acct_id`, `memo`
FROM `budget`
WHERE `date_begin` <= %s AND `date_end` >= %s