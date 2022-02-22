from pathlib import Path
from datetime import datetime
import os
import os.path
from abc import ABC, abstractmethod
import random
from wordcloud import WordCloud
import pandas as pd

from .word_cloud_raw import WordCloudRaw
from .morpheme_raw import MorphemeRaw

# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class SvPallet:
    __g_dictSourceMedium = {'default': '#000000',   # https://www.color-hex.com/
                            'youtube_display': '#960614',
                            'google_cpc': '#6b0000',
                            'facebook_cpi': '#205a86',
                            'facebook_cpc': '#140696',
                            'naver_cpc': '#4d6165',
                            'naver_organic': '#798984',
                            'naver_display': '#8db670',
                            'kakao_cpc': '#ffad60'}
    __g_dictPeriodWindow = {'tm': '#2ABA9C',  # this period
                            'lm': '#A0DBCF',  # last period
                            'ly': '#D6E2DF',  # year ago period
                            '2ly': '#e7298a'}  # 2 years ago period
    __g_sDefaultColor = '#000000'

    def __new__(cls):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)  # turn to decorator
        return super().__new__(cls)

    def __init__(self):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        super().__init__()

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __del__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        # logger.debug('__exit__')
        pass

    def get_word_color(self, s_source_medium=None):
        """
        :param s_source_medium:
        :return:
        """
        if s_source_medium is None:
            return self.__g_dictSourceMedium
        elif s_source_medium in self.__g_dictSourceMedium:  # list(self.__g_dictSourceMedium.keys()):
            return self.__g_sDefaultColor
        else:
            return self.__g_dictSourceMedium[s_source_medium]

    def get_period_window_color(self, s_period_window=None):
        """
        :param s_period_window:
        :return:
        """
        if s_period_window is None:
            return self.__g_dictPeriodWindow
        elif s_period_window in self.__g_dictPeriodWindow:  # list(self.__g_dictPeriodWindow.keys()):
            return self.__g_sDefaultColor
        else:
            return self.__g_dictPeriodWindow[s_period_window]


class WordCloudVisual(ABC):
    """
    https://hyunlee103.tistory.com/91  추상성 다형성
    # pip install wordcloud
    """
    _g_oSvDb = None
    _g_dictPeriod = {}
    _g_dictSamplingFreq = {'qtr': 'Q', 'mon': 'M', 'day': 'D'}
    _g_sFreqMode = 'D'
    _g_dictPeriodRaw = {'2ly': None, 'ly': None, 'lm': None, 'tm': None}
    _g_dictDictionary = {}

    def __init__(self, o_sv_db):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        if not o_sv_db:  # refer to an external db instance to minimize data class
            raise Exception('invalid db handler')
        self._g_oSvDb = o_sv_db

        o_sv_pallet = SvPallet()
        self._g_dictPalletSourceMedium = o_sv_pallet.get_word_color()
        del o_sv_pallet
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

    def _get_dictionary(self, n_word_id):
        if self._g_dictDictionary.get(n_word_id, 0):
            return self._g_dictDictionary[n_word_id]
        else:
            if n_word_id:
                lst_dictionary = self._g_oSvDb.executeQuery('getDictionaryByWordId', n_word_id)
                s_word = lst_dictionary[0]['word']
                self._g_dictDictionary[n_word_id] = s_word
            else:  # n_word_id is 0
                s_word = 'word_cloud'
            return s_word


# pd.set_option('display.max_columns', None) 후에 print(df)하면 모든 컬럼명이 출력됨
class WcMainVisual(WordCloudVisual):
    """
    match with ./templates/svload/ga_media_main.html
    """
    __g_sWordCloudImgPathRoot = 'word_cloud'
    __g_sWordCloudFontName = 'godoMaum'

    def load_df(self):
        for s_period in self._g_dictPeriod:  # list(self._g_dictPeriod.keys()):
            dt_first_date = self._g_dictPeriod[s_period][0]
            dt_last_date = self._g_dictPeriod[s_period][1]
            o_word_cloud_raw = WordCloudRaw(self._g_oSvDb)
            o_word_cloud_raw.set_period_dict(dt_start=dt_first_date, dt_end=dt_last_date)
            o_word_cloud_raw.set_freq(self._g_sFreqMode)
            o_word_cloud_raw.load_period()
            self._g_dictPeriodRaw[s_period] = o_word_cloud_raw

    def unset_image_dir(self, dict_config):
        s_media_file_path = dict_config['s_media_file_path']
        n_brand_id = dict_config['n_brand_id']
        s_word_cloud_img_path = os.path.join(s_media_file_path, self.__g_sWordCloudImgPathRoot, str(n_brand_id))
        if not os.path.exists(s_word_cloud_img_path):
            return
        try:
            for s_single_file in os.listdir(s_word_cloud_img_path):
                s_file_path_abs = os.path.join(s_word_cloud_img_path, s_single_file)
                if os.path.isfile(s_file_path_abs):
                    os.remove(s_file_path_abs)
        except PermissionError:
            logger.debug('file permission denied')

    def get_top_ranker(self, dict_config):
        s_static_file_path = dict_config['s_static_file_path']
        s_media_file_path = dict_config['s_media_file_path']
        s_media_url_root = dict_config['s_media_url_root']
        n_brand_id = dict_config['n_brand_id']
        # n_ga_view_id = dict_config['n_ga_view_id']
        s_word_cloud_img_path = os.path.join(s_media_file_path, self.__g_sWordCloudImgPathRoot, str(n_brand_id))
        s_word_cloud_img_url = s_media_url_root + self.__g_sWordCloudImgPathRoot + '/' + str(n_brand_id)
        if not os.path.isdir(s_word_cloud_img_path):
            Path(s_word_cloud_img_path).mkdir(parents=True, exist_ok=True)

        n_th_rank = dict_config['n_th_rank']
        dict_word_cloud_img_url = {}
        dict_top_word_by_freq = {}
        dict_misc_word_cnt = {}

        lst_period_window_wc_img = [s_single_file.replace('.png', '') for s_single_file in os.listdir(s_word_cloud_img_path)]
        lst_period_window_wc_img.reverse()  # to minimize iteration
        for s_period in dict_config['lst_period']:
            dict_top_word_by_freq[s_period], dict_misc_word_cnt[s_period] = self._g_dictPeriodRaw[s_period].top_ranker(n_th_rank)
            dt_first_date = self._g_dictPeriod[s_period][0]
            s_wc_img_filename = 'wc_' + dt_first_date.strftime("%Y_%m")
            del dt_first_date

            b_draw_word_cloud = True
            if s_period == 'tm':  # update daily if this month
                for s_single_old_wc_img_filename in lst_period_window_wc_img:
                    if s_wc_img_filename in s_single_old_wc_img_filename:
                        s_tm_wc_img_abs_path = os.path.join(s_word_cloud_img_path, s_wc_img_filename + '.png')
                        o_file = Path(s_tm_wc_img_abs_path)
                        if o_file.exists():
                            n_file_yyyymmdd = int(datetime.fromtimestamp(o_file.stat().st_mtime).strftime("%Y%m%d"))
                            n_today_yyyymmdd = int(datetime.today().strftime("%Y%m%d"))
                            if n_file_yyyymmdd >= n_today_yyyymmdd:
                                b_draw_word_cloud = False
                            else:
                                s_wc_img_filename = s_wc_img_filename + '_' + str(random.randint(10, 20))
                        break
            s_wc_img_filename = s_wc_img_filename + '.png'
            s_wc_img_abs_path = os.path.join(s_word_cloud_img_path, s_wc_img_filename)
            dict_word_cloud_img_url[s_period] = s_word_cloud_img_url + '/' + s_wc_img_filename
            if b_draw_word_cloud:
                o_wc = WordCloud(font_path=os.path.join(Path(__file__).resolve().parent.parent, 
                                            'wc_fonts', self.__g_sWordCloudFontName + '.ttf'),
                                 background_color="white", max_words=n_th_rank, max_font_size=300, width=1000,
                                 height=1000)
                dict_wc_raw = {}

                for s_word, dict_info in dict_top_word_by_freq[s_period].items():
                    dict_wc_raw[s_word] = dict_info['n_cnt']

                # o_wc.generate_from_frequencies(dict_top_word_by_freq[s_period])
                o_wc.generate_from_frequencies(dict_wc_raw)
                o_wc.to_file(s_wc_img_abs_path)
                del dict_wc_raw
                del o_wc

        lst_top_word_by_freq_trend = []
        for s_word, dict_word_info in dict_top_word_by_freq['tm'].items():  # n_tm_cnt
            if s_word in dict_top_word_by_freq['lm']:  #lst_lm_word:
                n_lm_cnt = dict_top_word_by_freq['lm'][s_word]['n_cnt']
            else:
                n_lm_cnt = 0

            if s_word in dict_top_word_by_freq['ly']:  # lst_ly_word:
                n_ly_cnt = dict_top_word_by_freq['ly'][s_word]['n_cnt']
            else:
                n_ly_cnt = 0

            lst_top_word_by_freq_trend.append(
                {'s_word': s_word, 'n_word_id': dict_top_word_by_freq['tm'][s_word]['n_word_id'],
                 'n_tm_freq': dict_word_info['n_cnt'], 'n_lm_freq': n_lm_cnt, 'n_ly_freq': n_ly_cnt})

        del dict_top_word_by_freq
        return {'lst_top_word_by_freq_trend': lst_top_word_by_freq_trend, 'dict_misc_word_cnt': dict_misc_word_cnt,
                'dict_word_cloud_img_url': dict_word_cloud_img_url}


class MorphemeVisual(WordCloudVisual):
    """
    https://hyunlee103.tistory.com/91  추상성 다형성
    """
    __g_DictMorphemeInfo = None
    __g_dfPeriodDataRaw = None

    def set_morpheme_lst(self, lst_morpheme_id):
        dict_tmp = {}
        if len(lst_morpheme_id):
            for n_word_id in lst_morpheme_id:
                dict_tmp[n_word_id] = self._get_dictionary(n_word_id)
            self.__g_DictMorphemeInfo = dict_tmp
        else:
            self.__g_DictMorphemeInfo = dict_tmp
        del dict_tmp

    def load_df(self):
        for n_morpheme_id, s_morpheme in list(self.__g_DictMorphemeInfo.items()):
            dt_first_date = self._g_dictPeriod['2ly'][0]
            dt_last_date = self._g_dictPeriod['tm'][1]
            o_morpheme_raw = MorphemeRaw(self._g_oSvDb)
            o_morpheme_raw.set_morpheme_dict(n_morpheme_id, s_morpheme)
            o_morpheme_raw.set_period_dict(dt_start=dt_first_date, dt_end=dt_last_date)
            o_morpheme_raw.load_period()
            self._g_dictPeriodRaw[n_morpheme_id] = o_morpheme_raw

    def retrieve_daily_chronicle_by_morpheme_ml(self, lst_item_line_color):
        """
        bokeh 그래프 데이터: 선택된 4개 형태소의 시계열
        data for multi line graph; _ml means multi-line
        :param lst_item_line_color:
        :return:
        """
        # n_th_to_display = len(lst_item_line_color)
        lst_line_label = []
        lst_line_color = []
        lst_series_cnt = []
        lst_x_label = None
        for n_morpheme_id, s_morpheme in list(self.__g_DictMorphemeInfo.items()):
            df_sampled = self._g_dictPeriodRaw[n_morpheme_id].get_sampling(self._g_sFreqMode)
            lst_line_label.append(s_morpheme)
            lst_line_color.append(lst_item_line_color.pop(0))
            lst_series_cnt.append(df_sampled['cnt'].tolist())
            lst_x_label = df_sampled.index.unique().tolist()
            del df_sampled
        return {'lst_line_label': lst_line_label, 'lst_line_color': lst_line_color,
                'lst_series_cnt': lst_series_cnt, 'lst_x_label': lst_x_label}

    def get_morpheme_id_by_morpheme(self, s_morpheme):
        lst_rec = []
        for s_morpheme_single in s_morpheme.split(','):
            lst_rec.extend(self._g_oSvDb.executeQuery('getDictionaryByWord', '%' + s_morpheme_single + '%'))
        dict_rst = {'b_error': False, 's_msg': None, 'lst_morpheme': lst_rec}
        return dict_rst
