SELECT `module_srl`, `word_srl`, `cnt`, `logdate`
FROM `wc_word_cnt`
WHERE `logdate` <= %s