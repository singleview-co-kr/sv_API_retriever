SELECT `referral`, `word_srl`, `cnt`, `logdate`
FROM `wc_word_cnt`
WHERE `word_srl` =  %s AND `logdate` >= %s AND `logdate` <= %s