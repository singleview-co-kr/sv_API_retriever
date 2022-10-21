SELECT `acct_id`, `memo`, `date_begin`, `date_end`
FROM `budget`
WHERE DATE(date_begin) BETWEEN %s AND %s