from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd

# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class PeriodWindow:
    __g_bPeriodDebugMode = False
    __g_oHttpRequest = None
    __g_dictSamplingFreq = {'qtr': 'Q', 'mon': 'M', 'day': 'D'}
    __g_dictPeriod = {'dt_today': None,  # datetime(2021, 2, 24),  # datetime.today(),
                      'dt_first_day_this_month': None,
                      'dt_first_day_month_ago': None,
                      'dt_last_day_month_ago': None,
                      'dt_first_day_year_ago': None,
                      'dt_last_day_year_ago': None,
                      'dt_first_day_2year_ago': None,
                      'dt_last_day_2year_ago': None,
                      's_cur_sampling_freq': None,  # for data sampling
                      's_cur_sampling_freq_btn': None,  # for data sampling
                      's_cur_period_window': None}

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

    def activate_debug(self):
        self.__g_bPeriodDebugMode = True

    def get_period_range(self, request, s_freq_mode=None):
        self.__g_oHttpRequest = request  # for session controller

        # yrmo URI is the first priority, request.session['s_data_yrmo'] is afterwards
        s_sampling_freq_mode = request.GET.get('freq', None)
        if s_sampling_freq_mode:
            request.session['s_sampling_freq_mode'] = s_sampling_freq_mode  # Set a session value
        else:
            s_sampling_freq_mode = request.session.get('s_sampling_freq_mode', None)
            if s_sampling_freq_mode is None:
                s_sampling_freq_mode = 'mon'  # default is monthly freq

        if s_freq_mode is not None:  # enforce freq mode if requested
            if s_freq_mode in list(self.__g_dictSamplingFreq.keys()):
                s_sampling_freq_mode = s_freq_mode

        try:
            # lst_freq_mode.index(s_sampling_freq_mode)
            self.__g_dictPeriod['s_cur_sampling_freq'] = self.__g_dictSamplingFreq[s_sampling_freq_mode]
            self.__g_dictPeriod['s_cur_sampling_freq_btn'] = s_sampling_freq_mode
        except ValueError:
            raise Exception('invalid freq mode')

        if s_sampling_freq_mode == 'mon':  # monthly mode
            self.__get_freq_window_monthly()
        if s_sampling_freq_mode == 'qtr':
            self.__get_freq_window_quarterly()
        if s_sampling_freq_mode == 'day':
            self.__get_freq_window_daily()
        return self.__g_dictPeriod

    def get_sampling_freq_ui(self):
        if self.__g_dictPeriod['s_cur_sampling_freq_btn'] is None:
            raise Exception('call get_period_range() first')
        # print(list(gDictSamplingFreqMode.keys())[list(gDictSamplingFreqMode.values()).index(dict_period['s_cur_sampling_freq'])])
        # begin - sampling freq btn UI
        dict_sampling_freq_mode = {}
        for s_btn_id in self.__g_dictSamplingFreq.keys():
            if s_btn_id == self.__g_dictPeriod['s_cur_sampling_freq_btn']:
                dict_sampling_freq_mode[s_btn_id] = 1
            else:
                dict_sampling_freq_mode[s_btn_id] = 0
        # end - sampling freq btn UI
        return dict_sampling_freq_mode

    def __get_freq_window_daily(self):
        # 일간 계산
        # datetime.today()로 날짜 정보 생성하면 pd.date_range() 실행할 때 아래와 같이 생성되는데
        # DatetimeIndex(['2021-07-01 13:41:58.771940'], dtype='datetime64[ns]', freq='D')
        # reindex() 실행 시 아래와 같은 데이터가 필요해서 null dataframe이 생성됨
        # DatetimeIndex(['2021-07-01'], dtype='datetime64[ns]', freq='D')
        dt_today_tmp = datetime.today()
        if dt_today_tmp.day == 1:  # should how last month for closing at the first day of month
            dt_today_tmp = dt_today_tmp - relativedelta(days=1)
        dt_today = datetime(dt_today_tmp.year, dt_today_tmp.month, dt_today_tmp.day, 0, 0, 0)
        del dt_today_tmp
        dt_first_day_this_month = dt_today.replace(day=1)
        lst_requested_daily_period = []
        s_requested_start_date = self.__g_oHttpRequest.GET.get('startDate', None)
        s_requested_end_date = self.__g_oHttpRequest.GET.get('endDate', None)
        if s_requested_start_date and s_requested_end_date:
            dt_requested_end_date = datetime.strptime(s_requested_end_date, '%Y%m%d')
            if dt_today <= dt_requested_end_date:
                dt_requested_end_date = self.__get_last_day_of_month(dt_today)
                s_requested_end_date = str(dt_requested_end_date.strftime('%Y%m%d'))
            lst_requested_daily_period.append(datetime.strptime(s_requested_start_date, '%Y%m%d'))
            lst_requested_daily_period.append(dt_requested_end_date)  # datetime.strptime(s_requested_end_date, '%Y%m%d'))
            self.__set_session_val('s_data_daily_period', s_requested_start_date + '-' + s_requested_end_date)
        else:
            s_requested_daily_period = self.__get_session_val('s_data_daily_period')
            if s_requested_daily_period:
                a_requested_daily_period_tmp = s_requested_daily_period.split('-')
                lst_requested_daily_period.append(datetime.strptime(a_requested_daily_period_tmp[0], '%Y%m%d'))
                lst_requested_daily_period.append(datetime.strptime(a_requested_daily_period_tmp[1], '%Y%m%d'))
            else:
                dt_last_day_this_month = dt_first_day_this_month + relativedelta(months=1) - relativedelta(days=1)
                lst_requested_daily_period.append(dt_first_day_this_month)
                lst_requested_daily_period.append(dt_last_day_this_month)

        if lst_requested_daily_period[0].month == lst_requested_daily_period[1].month:  # daily should be a month
            # this period
            dt_first_day_this_month = lst_requested_daily_period[0]
            dt_last_day_this_month = lst_requested_daily_period[1]
            # dt_source.replace(month=dt_source.month + 1, day=1) - relativedelta(days=1)
            dt_first_day_month_ago = dt_first_day_this_month - relativedelta(months=1)  # replace(day=1)
            dt_last_day_month_ago = dt_last_day_this_month - relativedelta(months=1)
            dt_first_day_year_ago = dt_first_day_this_month - relativedelta(years=1)  # replace(day=1)
            dt_last_day_year_ago = dt_last_day_this_month - relativedelta(years=1)
            dt_first_day_2year_ago = dt_first_day_this_month - relativedelta(years=2)  # replace(day=1)
            dt_last_day_2year_ago = dt_last_day_this_month - relativedelta(years=2)
            self.__g_dictPeriod['dt_today'] = dt_last_day_this_month
            self.__g_dictPeriod['dt_first_day_this_month'] = dt_first_day_this_month
            self.__g_dictPeriod['dt_first_day_month_ago'] = dt_first_day_month_ago
            self.__g_dictPeriod['dt_last_day_month_ago'] = dt_last_day_month_ago
            self.__g_dictPeriod['dt_first_day_year_ago'] = dt_first_day_year_ago
            self.__g_dictPeriod['dt_last_day_year_ago'] = dt_last_day_year_ago
            self.__g_dictPeriod['dt_first_day_2year_ago'] = dt_first_day_2year_ago
            self.__g_dictPeriod['dt_last_day_2year_ago'] = dt_last_day_2year_ago
            if dt_today.day == 1:
                self.__g_dictPeriod['s_cur_period_window'] = (dt_today - relativedelta(months=1)).strftime("%Y%m")
            else:
                self.__g_dictPeriod['s_cur_period_window'] = dt_today.strftime("%Y%m")
        else:
            pass

    def __get_freq_window_monthly(self):
        dt_today = datetime.today()
        s_requested_yr_mo_point = self.__g_oHttpRequest.GET.get('yrmo', None)
        if s_requested_yr_mo_point:
            self.__set_session_val('s_data_yrmo', s_requested_yr_mo_point)
        else:
            s_requested_yr_mo_point = self.__get_session_val('s_data_yrmo')
            if s_requested_yr_mo_point is None:
                dt_yesterday = dt_today - timedelta(1)
                s_requested_yr_mo_point = str(dt_yesterday.strftime('%Y%m'))

        if s_requested_yr_mo_point:
            dt_req_day = datetime.strptime(s_requested_yr_mo_point, '%Y%m')  # 2021, 2,)
            if int(dt_req_day.strftime('%Y%m')) <= int(dt_today.strftime('%Y%m')):  # get last day of month, if past
                dt_today = self.__get_last_day_of_month(dt_req_day)
            else:
                dt_today = self.__get_last_day_of_month(dt_today)  # get last day of this month, if future

            # begin - activate only for MTD debugging
            if self.__g_bPeriodDebugMode:
                from datetime import date
                dt_today = date(2021, 2, 26)
            # end - activate only for MTD debugging

            # 당월의 1일로 변경
            dt_first_day_this_month = dt_today.replace(day=1)
            # 전월의 마지막 날 구하기
            dt_last_day_month_ago = dt_first_day_this_month - relativedelta(days=1)
            # 전월의 첫째 날 구하기
            dt_first_day_month_ago = dt_last_day_month_ago.replace(day=1)
            # 전년 동월의 첫째 날 구하기
            dt_first_day_year_ago = dt_first_day_this_month - relativedelta(years=1)
            # 전년 동월의 마지막 날 구하기
            dt_last_day_year_ago = self.__get_last_day_of_month(dt_first_day_year_ago)
            # 전전년 동월의 첫째 날 구하기
            dt_first_day_2year_ago = dt_first_day_this_month - relativedelta(years=2)
            # 전전년 동월의 마지막 날 구하기
            dt_last_day_2year_ago = self.__get_last_day_of_month(dt_first_day_2year_ago)
            self.__g_dictPeriod['dt_today'] = dt_today
            self.__g_dictPeriod['dt_first_day_this_month'] = dt_first_day_this_month
            self.__g_dictPeriod['dt_last_day_month_ago'] = dt_last_day_month_ago
            self.__g_dictPeriod['dt_first_day_month_ago'] = dt_first_day_month_ago
            self.__g_dictPeriod['dt_first_day_year_ago'] = dt_first_day_year_ago
            self.__g_dictPeriod['dt_last_day_year_ago'] = dt_last_day_year_ago
            self.__g_dictPeriod['dt_first_day_2year_ago'] = dt_first_day_2year_ago
            self.__g_dictPeriod['dt_last_day_2year_ago'] = dt_last_day_2year_ago
            self.__g_dictPeriod['s_cur_period_window'] = dt_today.strftime("%Y%m")
            # dt_yesterday = dt_today - timedelta(1)
            # n_mtd_day_cnt = int(datetime.strftime(dt_yesterday, '%d'))
            # dict_period['n_mtd_day_cnt'] = n_mtd_day_cnt
            # dict_period['s_cur_period_window'] = dt_today.strftime("%Y%m")
            # logger.debug(self.__g_dictPeriod)

    def __get_freq_window_quarterly(self):
        s_requested_yr_qr_point = self.__g_oHttpRequest.GET.get('yrqr', None)
        if s_requested_yr_qr_point:  # this period
            n_this_year = int(s_requested_yr_qr_point[:4])
            n_this_qtr = int(s_requested_yr_qr_point[-2:])
            if n_this_qtr == 1:
                dt_this_period_start = datetime(n_this_year, 1, 1)
                dt_this_period_end = datetime(n_this_year, 3, 31)
            if n_this_qtr == 2:
                dt_this_period_start = datetime(n_this_year, 4, 1)
                dt_this_period_end = datetime(n_this_year, 6, 30)
            if n_this_qtr == 3:
                dt_this_period_start = datetime(n_this_year, 7, 1)
                dt_this_period_end = datetime(n_this_year, 9, 30)
            if n_this_qtr == 4:
                dt_this_period_start = datetime(n_this_year, 10, 1)
                dt_this_period_end = datetime(n_this_year, 12, 31)
        else:  # designated period
            dt_this_period_start = pd.to_datetime(
                datetime.today() - pd.tseries.offsets.QuarterBegin(startingMonth=1)).date()
            dt_this_period_start = pd.to_datetime(dt_this_period_start)
            dt_this_period_end = pd.to_datetime(
                datetime.today() + pd.tseries.offsets.QuarterEnd()).date()
            dt_this_period_end = pd.to_datetime(dt_this_period_end)
            n_this_year = int(dt_this_period_end.year)
            quarter = pd.Timestamp(dt_this_period_end).quarter
            s_requested_yr_qr_point = str(n_this_year) + '0' + str(quarter)

        # prev period
        dt_prev_period_end = dt_this_period_start - timedelta(days=1)
        dt_prev_period_start = pd.to_datetime(
            pd.to_datetime(dt_prev_period_end) - pd.tseries.offsets.QuarterBegin(startingMonth=1)).date()
        dt_prev_period_start = pd.to_datetime(dt_prev_period_start)
        dt_prev_period_end = pd.to_datetime(
            pd.to_datetime(dt_prev_period_start) + pd.tseries.offsets.QuarterEnd(startingMonth=3)).date()
        dt_prev_period_end = pd.to_datetime(dt_prev_period_end)

        # ly period
        dt_ly_period_start = dt_this_period_start.replace(year=(n_this_year - 1))
        dt_ly_period_end = dt_this_period_end.replace(year=(n_this_year - 1))
        # 2ly period
        dt_2ly_period_start = dt_this_period_start.replace(year=(n_this_year - 2))
        dt_2ly_period_end = dt_this_period_end.replace(year=(n_this_year - 2))

        self.__g_dictPeriod['dt_first_day_this_month'] = dt_this_period_start
        self.__g_dictPeriod['dt_today'] = dt_this_period_end
        self.__g_dictPeriod['dt_first_day_month_ago'] = dt_prev_period_start
        self.__g_dictPeriod['dt_last_day_month_ago'] = dt_prev_period_end
        self.__g_dictPeriod['dt_first_day_year_ago'] = dt_ly_period_start
        self.__g_dictPeriod['dt_last_day_year_ago'] = dt_ly_period_end
        self.__g_dictPeriod['dt_first_day_2year_ago'] = dt_2ly_period_start
        self.__g_dictPeriod['dt_last_day_2year_ago'] = dt_2ly_period_end
        self.__g_dictPeriod['s_cur_period_window'] = s_requested_yr_qr_point

    def __set_session_val(self, s_key, s_val):
        self.__g_oHttpRequest.session[s_key] = s_val

    def __get_session_val(self, s_key):
        return self.__g_oHttpRequest.session.get(s_key, None)

    def __get_last_day_of_month(self, dt_source):
        if dt_source.month == 12:
            return dt_source.replace(day=31)
        return dt_source.replace(month=dt_source.month + 1, day=1) - relativedelta(days=1)
