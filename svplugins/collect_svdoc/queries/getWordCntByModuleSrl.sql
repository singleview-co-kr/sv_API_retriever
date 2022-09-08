SELECT `referral`, `document_srl`, `word_srl`, `cnt`
FROM `wc_word_cnt`
WHERE `module_srl` = %s 
