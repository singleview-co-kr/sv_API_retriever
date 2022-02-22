SELECT `referral`, `word_srl`, `cnt`, `logdate`
FROM `wc_word_cnt`
WHERE `logdate` >= %s AND `logdate` <= %s