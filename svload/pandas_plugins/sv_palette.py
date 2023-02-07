# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class SvPalette:
    """ # https://www.color-hex.com/ """
    # __g_lstSkuId = None

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
        # logger.debug('__del__')
        pass
    
    def get_single_color_lst(self):
        return ['#6CBDAC']

    def get_serial_color_lst(self):
        return ['#D6E2DF', '#A4C8C1', '#6CBDAC', '#079476']

    def get_source_medium_color_dict(self):
        return {'default': '#000000',  
                'youtube_display': '#960614',
                'google_cpc': '#6b0000',
                'facebook_cpi': '#205a86',
                'facebook_cpc': '#140696',
                'naver_cpc': '#4d6165',
                'naver_organic': '#798984',
                'naver_display': '#8db670',
                'kakao_cpc': '#ffad60'}

    def get_source_color_dict(self):
        return {'default': '#000000',
                'youtube': '#960614',
                'google': '#6b0000',
                'facebook': '#205a86',
                'instagram': '#140696',
                'naver': '#0099ff',
                'twitter': '#798984'}
    
    def get_naver_mdeium_color_dict(self):
        return {'blog': '#960614',
                'news': '#6b0000',
                'cafe': '#205a86',
                'kin': '#140696',
                'webkr': '#4d6165'}

    def get_period_window_color_dict(self):
        return {'tm': '#2ABA9C',  # this period
                'lm': '#A0DBCF',  # last period
                'ly': '#D6E2DF',  # year ago period
                '2ly': '#e7298a'}  # 2 years ago period
