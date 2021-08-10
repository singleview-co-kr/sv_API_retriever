class getOldDocumentLogByDocSrl:
    __g_sSqlStatement = None

    def initialize(self, dict_param):
        self.__g_sSqlStatement = "SELECT document_srl " \
                                 "FROM `wc_document_log` " \
                                 "WHERE `document_srl` IN ({in_clause})".format(in_clause=dict_param['s_updated_doc_srls'])

    def __del__(self):
        pass

    def __str__(self):
        return self.__g_sSqlStatement
