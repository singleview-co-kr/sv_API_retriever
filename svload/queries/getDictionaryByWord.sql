SELECT `word_srl`, `word`, `b_ignore`
FROM `wc_collected_dictionary`
WHERE `word` LIKE %s and `b_ignore` != 1