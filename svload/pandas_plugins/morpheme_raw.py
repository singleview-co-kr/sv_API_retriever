# from datetime import datetime
import pandas as pd


# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class MorphemeRaw:
    __g_sClassId = None
    __g_oSvDb = None

    __g_dtDesignatedFirstDate = None  # 추출 기간 시작일
    __g_dtDesignatedLastDate = None  # 추출 기간 종료일
    __g_lstAllowedSamplingFreq = ['Q', 'M', 'W', 'D']
    __g_dictDictionary = {}
    __g_dictMorphemeDf = {}

    # def __new__(cls):
    #    # print(__file__ + ':' + sys._getframe().f_code.co_name)  # turn to decorator
    #    return super().__new__(cls)

    def __init__(self, o_sv_db):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        if not o_sv_db:  # refer to an external db instance to minimize data class
            raise Exception('invalid db handler')
        self.__g_oSvDb = o_sv_db
        self.__g_sClassId = id(self)
        self.__g_dictMorphemeDf[self.__g_sClassId] = None
        self.__g_dictDictionary[self.__g_sClassId] = {'n_morpheme_id': None, 's_morpheme': None}
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

    def set_morpheme_dict(self, n_morpheme_id, s_morpheme):
        # self.__g_dictDictionary = dict_morpheme_info
        self.__g_dictDictionary[self.__g_sClassId]['n_morpheme_id'] = n_morpheme_id
        self.__g_dictDictionary[self.__g_sClassId]['s_morpheme'] = s_morpheme

    def set_period_dict(self, dt_start, dt_end):
        self.__g_dtDesignatedFirstDate = dt_start
        self.__g_dtDesignatedLastDate = dt_end

    def load_period(self):
        if self.__g_dtDesignatedFirstDate is None or self.__g_dtDesignatedLastDate is None:
            raise Exception('not ready to load dataframe to analyze')

        lst_raw_data = self.__g_oSvDb.executeQuery('getMorphemeChronicle',
                                                   self.__g_dictDictionary[self.__g_sClassId]['n_morpheme_id'],
                                                   self.__g_dtDesignatedFirstDate, self.__g_dtDesignatedLastDate)
        if len(lst_raw_data) == 0:
            df_period_data_raw = self.__set_nullify_dataframe()
        else:
            df_period_data_raw = pd.DataFrame(lst_raw_data)
        del lst_raw_data

        # ensure logdate field to datetime format
        df_period_data_raw['logdate'] = pd.to_datetime(df_period_data_raw['logdate'])
        self.__g_dictMorphemeDf[self.__g_sClassId] = df_period_data_raw

    def get_sampling(self, s_freq='D'):
        self.__g_dictMorphemeDf[self.__g_sClassId].set_index('logdate', inplace=True)
        # to fill out empty element
        idx_full_period = pd.date_range(start=self.__g_dtDesignatedFirstDate, end=self.__g_dtDesignatedLastDate,
                                        freq=s_freq)
        df_resample = self.__g_dictMorphemeDf[self.__g_sClassId].resample(s_freq).sum()
        return df_resample.reindex(idx_full_period, fill_value=0)

    def __set_nullify_dataframe(self):
        lst_blank_word_cnt = [[1, 0, 1, self.__g_dtDesignatedFirstDate]]  # set word count null
        df_blank = pd.DataFrame(lst_blank_word_cnt)
        df_blank.rename(columns={0: 'referral', 1: 'word_srl', 2: 'cnt', 3: 'logdate'}, inplace=True)
        del lst_blank_word_cnt
        return df_blank
