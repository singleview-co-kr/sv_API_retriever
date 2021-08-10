SELECT `referral`, `document_srl`, `word_srl`, `cnt`
FROM `wc_word_cnt`
WHERE `logdate` >= %s AND `logdate` <= %s