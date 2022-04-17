class getStatusById:
    __g_sSqlStatement = None

    def initialize(self, dict_param):
        self.__g_sSqlStatement = "SELECT * " \
                                 "FROM `twt_status` " \
                                 "WHERE `status_id` IN ({in_clause})".format(in_clause=dict_param['s_new_status_ids'])

    def __del__(self):
        pass

    def __str__(self):
        return self.__g_sSqlStatement
