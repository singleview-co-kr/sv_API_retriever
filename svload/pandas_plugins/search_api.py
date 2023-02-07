import pandas as pd
from abc import ABC
from abc import abstractmethod

from svcommon.sv_nvsearch import SvNvsearch
from svcommon.sv_campaign_parser import SvCampaignParser

# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class SearchApiVisual(ABC):
    """
    https://hyunlee103.tistory.com/91  추상성 다형성
    """
    _g_oSvDb = None
    _g_dictPeriod = {}
    _g_dictSamplingFreq = {'qtr': 'Q', 'mon': 'M', 'day': 'D'}
    _g_sFreqMode = 'D'
    _g_dictPeriodRaw = {'2ly': None, 'ly': None, 'lm': None, 'tm': None}
    _g_dictSource = {'default': '#000000',   # https://www.color-hex.com/
                     'youtube': '#960614',
                     'google': '#6b0000',
                     'facebook': '#205a86',
                     'instagram': '#140696',
                     'naver': '#0099ff',
                     'twitter': '#798984'}
    _g_dictMedium = {'blog': '#960614',
                     'news': '#6b0000',
                     'cafe': '#205a86',
                     'kin': '#140696',
                     'webkr': '#4d6165'}
    

    def __init__(self, o_sv_db):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        if not o_sv_db:  # refer to an external db instance to minimize data class
            raise Exception('invalid db handler')
        self._g_oSvDb = o_sv_db
        super().__init__()

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __del__(self):
        for s_period in self._g_dictPeriod:  # list(self._g_dictPeriod.keys()):
            try:
                del self._g_dictPeriodRaw[s_period]
            except KeyError:
                pass

    @abstractmethod
    def load_df(self):
        pass

    def set_period_dict(self, dict_period, lst_switch=None):
        """
        dtDesignatedFirstDate = self._g_dictPeriod['2ly'][0]
        dtDesignatedLastDate = self._g_dictPeriod['tm'][1]
        :param dict_period:
        :param lst_switch:
        :return:
        """
        if not dict_period['dt_first_day_year_ago'] or not dict_period['dt_today']:
            raise Exception('invalid data period')
        if lst_switch is None:
            raise Exception('invalid period switch')

        dict_period_switch = {
            'tm': ['dt_first_day_this_month', 'dt_today'],
            'lm': ['dt_first_day_month_ago', 'dt_last_day_month_ago'],
            'ly': ['dt_first_day_year_ago', 'dt_last_day_year_ago'],
            '2ly': ['dt_first_day_2year_ago', 'dt_last_day_2year_ago'],
        }

        for s_period_window in lst_switch:
            self._g_dictPeriod[s_period_window] = \
                [dict_period[dict_period_switch[s_period_window][0]], dict_period[dict_period_switch[s_period_window][1]]]

        del dict_period_switch

    def set_freq(self, dict_freq):
        for s_freq, b_activated in dict_freq.items():
            if b_activated:
                self._g_sFreqMode = self._g_dictSamplingFreq[s_freq]
                break
        # self._g_sFreqMode = self._g_dictSamplingFreq['day']  # for daily debug

    # def _get_dictionary(self, n_word_id):
    #     if self._g_dictDictionary.get(n_word_id, 0):
    #         return self._g_dictDictionary[n_word_id]
    #     else:
    #         if n_word_id:
    #             lst_dictionary = self._g_oSvDb.executeQuery('getDictionaryByWordId', n_word_id)
    #             s_word = lst_dictionary[0]['word']
    #             self._g_dictDictionary[n_word_id] = s_word
    #         else:  # n_word_id is 0
    #             s_word = 'word_cloud'
    #         return s_word


# pd.set_option('display.max_columns', None) 후에 print(df)하면 모든 컬럼명이 출력됨
class SearcgApiFreqTrendVisual(SearchApiVisual):
    """
    match with ./templates/svload/ga_media_main.html
    """
    # __g_sWordCloudImgPathRoot = 'word_cloud'
    # __g_sWordCloudFontName = 'godoMaum'

    def load_df(self):
        for s_period in self._g_dictPeriod:  # list(self._g_dictPeriod.keys()):
            dt_first_date = self._g_dictPeriod[s_period][0]
            dt_last_date = self._g_dictPeriod[s_period][1]
            o_search_api_raw = SearchApiRaw(self._g_oSvDb)
            o_search_api_raw.set_period_dict(dt_start=dt_first_date, dt_end=dt_last_date)
            o_search_api_raw.set_freq(self._g_sFreqMode)
            o_search_api_raw.load_period()
            self._g_dictPeriodRaw[s_period] = o_search_api_raw
    
    def retrieve_daily_chronicle_by_source_ml(self, s_period_window):
        """
        bokeh 그래프 데이터: 선택된 시계열
        data for multi line graph; _ml means multi-line
        :param s_period_window:
        :return:
        """
        lst_line_label = []
        lst_line_color = []
        lst_series_cnt = []
        lst_x_label = None
        if s_period_window in self._g_dictPeriod:
            dict_sampled_by_source = self._g_dictPeriodRaw[s_period_window].get_sampling_by_source_srl(self._g_sFreqMode)

            lst_x_label = dict_sampled_by_source['idx_full_period']
            del dict_sampled_by_source['idx_full_period']

            for s_source_lbl, lst_daily_freq in dict_sampled_by_source.items():
                lst_line_label.append(s_source_lbl)
                lst_line_color.append(self._g_dictSource[s_source_lbl])
                lst_series_cnt.append(lst_daily_freq)
                # lst_x_label = df_sampled.index.unique().tolist()
            del dict_sampled_by_source
        return {'lst_line_label': lst_line_label, 'lst_line_color': lst_line_color,
                'lst_series_cnt': lst_series_cnt, 'lst_x_label': lst_x_label}

    def retrieve_daily_chronicle_by_source_sb(self, s_period_window):
        """
        bokeh 그래프 데이터: 선택된 누적 막대
        data for multi line graph; _sb means stacked bar
        :param s_period_window:
        :return:
        """
        lst_bar_color = []
        lst_x_label = []
        n_y_max_val = 0
        if s_period_window in self._g_dictPeriod:
            dict_sampled_by_source = self._g_dictPeriodRaw[s_period_window].get_sampling_by_source_srl(self._g_sFreqMode)
            # retrieve x-lbl
            for pd_dt in dict_sampled_by_source['idx_full_period']:
                lst_x_label.append(str(pd_dt)[:10])
            del dict_sampled_by_source['idx_full_period']

            for s_source_lbl, lst_daily_freq in dict_sampled_by_source.items():
                n_y_max_val += max(lst_daily_freq)
                lst_bar_color.append(self._g_dictSource[s_source_lbl])

        return {'data_body': dict_sampled_by_source, 'y_label': lst_x_label,
                'y_max_val': int(n_y_max_val*1.01), 'lst_x_label': lst_x_label, 'lst_palette': lst_bar_color}

class SearchApiRaw:
    __g_sClassId = None
    __g_oSvDb = None

    __g_dtDesignatedFirstDate = None  # 추출 기간 시작일
    __g_dtDesignatedLastDate = None  # 추출 기간 종료일
    __g_lstAllowedSamplingFreq = ['Q', 'M', 'W', 'D']
    __g_sFreqMode = 'D'  # default freq is daily
    __g_dictStatus = {}

    # def __new__(cls):
    #    # print(__file__ + ':' + sys._getframe().f_code.co_name)  # turn to decorator
    #    return super().__new__(cls)

    def __init__(self, o_sv_db):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        if not o_sv_db:  # refer to an external db instance to minimize data class
            raise Exception('invalid db handler')
        self.__g_oSvDb = o_sv_db
        self.__g_sClassId = id(self)
        self.__g_dictStatus[self.__g_sClassId] = None
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

    # def set_morpheme_dict(self, n_morpheme_id, s_morpheme):
    #     self.__g_dictDictionary[self.__g_sClassId]['n_morpheme_id'] = n_morpheme_id
    #     self.__g_dictDictionary[self.__g_sClassId]['s_morpheme'] = s_morpheme

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

        o_sv_campaign_parser = SvCampaignParser()
        dict_source_title_id = o_sv_campaign_parser.get_source_id_title_dict(b_inverted=True)
        del o_sv_campaign_parser

        # begin - Retrieve twitter API collection count
        lst_raw_data = self.__g_oSvDb.executeQuery('getTwitterApiCntByLogdatePeriod',
                                                    self.__g_dtDesignatedFirstDate, self.__g_dtDesignatedLastDate)
        if len(lst_raw_data) == 0:
            df_period_data_raw_twt = self.__set_nullify_dataframe(dict_source_title_id['twitter'])
        else:
            for dict_single_row in lst_raw_data:
                dict_single_row['source_srl'] = dict_source_title_id['twitter']
                dict_single_row['media_id'] = 0
            df_period_data_raw_twt = pd.DataFrame(lst_raw_data)    
        # ensure logdate field to datetime format
        df_period_data_raw_twt['logdate'] = pd.to_datetime(df_period_data_raw_twt['logdate'])
        # end - Retrieve twitter API collection count

        # begin - Retrieve naver search API collection count
        o_sv_nvsearch = SvNvsearch()
        dict_media_lbl_id = o_sv_nvsearch.get_media_lbl_id_dict()
        del o_sv_nvsearch

        lst_gross_naver_raw_data = []
        for s_media_lbl, n_media_id in dict_media_lbl_id.items():
            # print(s_media_lbl, n_media_id)
            if s_media_lbl == 'blog' or s_media_lbl == 'news':  # news blog > logdate, 
                lst_raw_data = self.__g_oSvDb.executeQuery('getNvrSearchApiCntByLogdatePeriod',
                                                            self.__g_dtDesignatedFirstDate, self.__g_dtDesignatedLastDate,
                                                            n_media_id)
            else:  # other media > regdate 
                lst_raw_data = self.__g_oSvDb.executeQuery('getNvrSearchApiCntByRegdatePeriod',
                                                            self.__g_dtDesignatedFirstDate, self.__g_dtDesignatedLastDate,
                                                            n_media_id)
            if lst_raw_data and 'err_code' in lst_raw_data[0].keys():  # for an initial stage; no table
                pass
            else:
                lst_gross_naver_raw_data += lst_raw_data
            del lst_raw_data
        del dict_media_lbl_id
        
        if len(lst_gross_naver_raw_data) == 0:
            df_period_data_raw_nvr = self.__set_nullify_dataframe(dict_source_title_id['naver'])
        else:
            for dict_single_row in lst_gross_naver_raw_data:
                dict_single_row['source_srl'] = dict_source_title_id['naver']
            df_period_data_raw_nvr = pd.DataFrame(lst_gross_naver_raw_data)
        # ensure logdate field to datetime format
        df_period_data_raw_nvr['logdate'] = pd.to_datetime(df_period_data_raw_nvr['logdate'])
        del lst_gross_naver_raw_data
        # end - Retrieve naver search API collection count

        del dict_source_title_id
        self.__g_dictStatus[self.__g_sClassId] = pd.concat([df_period_data_raw_twt, df_period_data_raw_nvr])  
    
    def get_sampling_by_source_srl(self, s_freq='D'):
        o_sv_campaign_parser = SvCampaignParser()
        dict_source_id_title = o_sv_campaign_parser.get_source_id_title_dict()
        del o_sv_campaign_parser
        # to fill out empty element
        idx_full_period = pd.date_range(start=self.__g_dtDesignatedFirstDate, end=self.__g_dtDesignatedLastDate,
                                        freq=s_freq)
        # for tup_ in idx_full_period.to_list():
        #     print(type(tup_))
        self.__g_dictStatus[self.__g_sClassId].set_index('logdate', inplace=True)
        df_grouper = self.__g_dictStatus[self.__g_sClassId].groupby([pd.Grouper(freq='D'), 'source_srl']) #resample(s_freq).sum()
        dict_freq_by_source = {}
        for n_source_srl, pd_series in df_grouper['source_srl'].count().unstack().items():
            dict_freq_by_source[dict_source_id_title[n_source_srl]] = \
                pd_series.to_frame().fillna(0).reindex(idx_full_period, fill_value=0)[n_source_srl].values.tolist()
        dict_freq_by_source['idx_full_period'] = idx_full_period.to_list()
        del idx_full_period
        del df_grouper
        del dict_source_id_title
        return dict_freq_by_source

    def __set_nullify_dataframe(self, n_source_srl):
        lst_blank_word_cnt = [[0, 0, self.__g_dtDesignatedFirstDate, n_source_srl]]  # set null dataframe
        df_blank = pd.DataFrame(lst_blank_word_cnt)
        df_blank.rename(columns={0: 'morpheme_srl', 1: 'media_id', 2: 'logdate', 3: 'source_srl'}, inplace=True)
        del lst_blank_word_cnt
        return df_blank