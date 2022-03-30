class getEdiLtmartLogByPeriod:
    __g_sSqlStatement = None

    def initialize(self, dict_param):
        self.__g_sSqlStatement = "SELECT R1.* FROM( " \
                                 "SELECT `id`, `item_id`, `branch_id`, `logdate`, `qty`, `amnt` " \
                                 "FROM `edi_ltmart_daily_log` " \
                                 "WHERE `logdate` >= '{s_period_start}' AND " \
                                 "`logdate` <= '{s_period_end}' ORDER BY id ASC" \
                                 ") R1 LIMIT {n_limit} OFFSET {n_offset}".format(s_period_start=dict_param['s_period_start'],
                                                                                 s_period_end=dict_param['s_period_end'],
                                                                                 n_offset=dict_param['n_offset'],
                                                                                 n_limit=dict_param['n_limit'])

    def __del__(self):
        pass

    def __str__(self):
        return self.__g_sSqlStatement
