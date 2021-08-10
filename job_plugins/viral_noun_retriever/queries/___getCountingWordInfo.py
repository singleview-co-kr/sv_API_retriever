class getCountingWordInfo:
    __g_sSqlStatement = None

    def initialize(self, dict_param):
        self.__g_sSqlStatement = "SELECT `word_srl`, `word`, `b_ignore` " \
                                 "FROM `viral_dictionary` " \
                                 "WHERE `word_srl` IN ({in_clause})".format(in_clause=dict_param['s_word_srls'])

    def __del__(self):
        pass

    def __str__(self):
        return self.__g_sSqlStatement
