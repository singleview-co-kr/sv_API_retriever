# from datetime import datetime
import pandas as pd


# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class WordCloudRaw:
    __g_sClassId = None
    __g_oSvDb = None

    __g_dtDesignatedFirstDate = None  # 추출 기간 시작일
    __g_dtDesignatedLastDate = None  # 추출 기간 종료일
    __g_lstAllowedSamplingFreq = ['Q', 'M', 'W', 'D']
    __g_sFreqMode = 'D'  # default freq is daily
    __g_dictWordCloudDf = {}
    __g_dictDictionary = {}

    # def __new__(cls):
    #    # print(__file__ + ':' + sys._getframe().f_code.co_name)  # turn to decorator
    #    return super().__new__(cls)

    def __init__(self, o_sv_db):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        if not o_sv_db:  # refer to an external db instance to minimize data class
            raise Exception('invalid db handler')
        self.__g_oSvDb = o_sv_db
        self.__g_sClassId = id(self)
        self.__g_dictWordCloudDf[self.__g_sClassId] = None
        self.__g_dictDictionary[self.__g_sClassId] = {}
        super().__init__()

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __del__(self):
        # logger.debug('__del__')
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        # logger.debug('__exit__')
        pass

    def set_period_dict(self, dt_start, dt_end):
        self.__g_dtDesignatedFirstDate = dt_start
        self.__g_dtDesignatedLastDate = dt_end

    def set_freq(self, s_freq_mode):
        if s_freq_mode in self.__g_lstAllowedSamplingFreq:
            self.__g_sFreqMode = s_freq_mode
        else:
            pass  # default is already 'D'
        ######################
        self.__g_sFreqMode = 'D'  # for daily debug
        ######################

    def load_period(self):
        if self.__g_dtDesignatedFirstDate is None or self.__g_dtDesignatedLastDate is None:
            raise Exception('not ready to load dataframe to analyze')

        lst_raw_data = self.__g_oSvDb.executeQuery('getWordCount',
                                                   self.__g_dtDesignatedFirstDate, self.__g_dtDesignatedLastDate)
        if lst_raw_data and 'err_code' in lst_raw_data.pop().keys():  # for an initial stage; no table
            lst_raw_data = []

        if len(lst_raw_data) == 0:
            df_period_data_raw = self.__set_nullify_dataframe()
        else:
            df_period_data_raw = pd.DataFrame(lst_raw_data)
        del lst_raw_data

        # ensure logdate field to datetime format
        df_period_data_raw['logdate'] = pd.to_datetime(df_period_data_raw['logdate'])
        self.__g_dictWordCloudDf[self.__g_sClassId] = df_period_data_raw

    def top_ranker(self, n_th_rank):
        df_by_word_freq = self.__g_dictWordCloudDf[self.__g_sClassId]
        n_whole_word_cnt = len(df_by_word_freq.index)
        del df_by_word_freq['referral']
        df_by_word_freq = df_by_word_freq.groupby(['word_srl']).sum()
        df_by_word_freq = df_by_word_freq.sort_values(by=['cnt'], axis=0, ascending=False)  # .head(n_th_rank)
        dict_top_word_by_freq = {}
        n_rank = 1
        for n_word_id, row in df_by_word_freq.iterrows():
            dict_word_info = self.__get_dictionary(n_word_id)
            if dict_word_info['s_ignore'] == '0':
                dict_top_word_by_freq[dict_word_info['s_word']] = {'n_word_id': n_word_id, 'n_cnt': row.cnt}
                if n_rank >= n_th_rank:
                    break
                else:
                    n_rank = n_rank + 1
        del df_by_word_freq

        if n_whole_word_cnt < n_th_rank:
            n_misc_word_cnt = 0
        else:
            n_misc_word_cnt = n_whole_word_cnt - n_th_rank
        return dict_top_word_by_freq, n_misc_word_cnt

    def __get_dictionary(self, n_word_id):
        # try:
        if self.__g_dictDictionary[self.__g_sClassId].get(n_word_id, 0):
            return self.__g_dictDictionary[self.__g_sClassId][n_word_id]
        # except KeyError:
        else:
            if n_word_id:
                lst_dictionary = self.__g_oSvDb.executeQuery('getDictionaryByWordId', n_word_id)
                s_word = lst_dictionary[0]['word']
                s_ignore = lst_dictionary[0]['b_ignore']
                self.__g_dictDictionary[self.__g_sClassId][n_word_id] = {'s_word': s_word, 's_ignore': s_ignore}
            else:  # n_word_id is 0
                self.__g_dictDictionary[self.__g_sClassId][n_word_id] = {'s_word': 'word_cloud', 's_ignore': '0'}
            return self.__g_dictDictionary[self.__g_sClassId][n_word_id]

    def __set_nullify_dataframe(self):
        lst_blank_word_cnt = [[1, 0, 1, self.__g_dtDesignatedFirstDate]]  # set word count null
        df_blank = pd.DataFrame(lst_blank_word_cnt)
        df_blank.rename(columns={0: 'referral', 1: 'word_srl', 2: 'cnt', 3: 'logdate'}, inplace=True)
        del lst_blank_word_cnt
        return df_blank
