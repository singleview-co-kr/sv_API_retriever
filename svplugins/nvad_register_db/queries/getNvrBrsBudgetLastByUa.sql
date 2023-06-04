SELECT `date_end`
FROM `budget`
WHERE `ua` = %s
ORDER BY `date_end` DESC LIMIT 1