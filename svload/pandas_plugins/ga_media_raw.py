import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from svcommon.sv_campaign_parser import SvCampaignParser

# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class GaSourceMediaRaw:
    __g_sClassId = None
    __g_oSvDb = None
    __g_oSvCampaignParser = None
    __g_dtDesignatedFirstDate = None  # 추출 기간 시작일
    __g_dtDesignatedLastDate = None  # 추출 기간 종료일
    __g_dfPeriodDataRaw = None  # 추출한 기간 데이터 원본
    __g_lstAllowedSamplingFreq = ['Q', 'M', 'W', 'D']
    __g_sFreqMode = 'D'  # default freq is daily
    __g_nBrandId = -1
    __g_fFullPeriodEstFactor = 1  # 전기간 마감 예상 가중치
    __g_dfCompressedByFreqMode = None  # 빈도에 따라 압축된 데이터
    __g_dictAggregatedDf = {}  # 지표별 합산 데이터
    __g_dictBudgetInfo = {}  # 예산과 대행사 수수료 정보
    __g_idxFullPeriod = None  # 설정한 기간의 완전한 일자 목록
    __g_dtToday = None

    # __g_dictSamplingFreq = {'qtr': 'Q', 'mon': 'M', 'day': 'D'}

    # def __new__(cls):
    #    # print(__file__ + ':' + sys._getframe().f_code.co_name)  # turn to decorator
    #    return super().__new__(cls)

    def __init__(self, o_sv_db):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        if not o_sv_db:  # refer to an external db instance to minimize data class
            raise Exception('invalid db handler')
        self.__g_oSvDb = o_sv_db
        self.__g_oSvCampaignParser = SvCampaignParser()
        self.__g_sClassId = id(self)
        self.__g_dictAggregatedDf[self.__g_sClassId] = {'mtd': None, 'full': None}
        self.__g_dtToday = datetime.today().date()
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

    def set_brand_id(self, n_brand_id):
        self.__g_nBrandId = n_brand_id

    def load_period(self, lst_retrieve_attrs):
        if self.__g_dtDesignatedFirstDate is None or self.__g_dtDesignatedLastDate is None:
            raise Exception('not ready to load dataframe to analyze')
        self.__construct_dataframe()  # retrieve raw data
        # self.__compress_data_by_freq_mode()  # compress by freq mode

    def get_budget_info(self, s_source_medium=None):
        if s_source_medium is None:
            return self.__g_dictBudgetInfo
        else:
            return self.__g_dictBudgetInfo[s_source_medium]

    def get_period_numeric_by_index(self, s_column_title=None):
        if s_column_title is None:
            return self.__g_dfCompressedByFreqMode
        else:
            return self.__g_dfCompressedByFreqMode[s_column_title]

    def get_gross_numeric_by_index_mtd(self, s_column_title=None):
        if s_column_title is None:
            return self.__g_dictAggregatedDf[self.__g_sClassId]['mtd']
        else:
            return self.__g_dictAggregatedDf[self.__g_sClassId]['mtd'][s_column_title]

    def get_gross_numeric_by_index_full(self, s_column_title=None):
        if s_column_title is None:
            return self.__g_dictAggregatedDf[self.__g_sClassId]['full']
        else:
            return self.__g_dictAggregatedDf[self.__g_sClassId]['full'][s_column_title]

    def get_sv_campaign_info(self, s_sv_camp_name):
        dict_camp_rst = self.__g_oSvCampaignParser.parse_campaign_code(s_sv_camp_name)
        if dict_camp_rst['source'] == 'unknown' or dict_camp_rst['rst_type'] == '' or \
                dict_camp_rst['medium'] == '':
            b_valid_sv_campaign_code = False
        else:
            b_valid_sv_campaign_code = True

        if b_valid_sv_campaign_code:
            df_campaign = self.__g_dfPeriodDataRaw.loc[
                (self.__g_dfPeriodDataRaw['media_source'] == dict_camp_rst['source']) &
                (self.__g_dfPeriodDataRaw['media_rst_type'] == dict_camp_rst['rst_type']) &
                (self.__g_dfPeriodDataRaw['media_media'] == dict_camp_rst['medium']) &
                (self.__g_dfPeriodDataRaw['media_camp1st'] == dict_camp_rst['campaign1st']) &
                (self.__g_dfPeriodDataRaw['media_camp2nd'] == dict_camp_rst['campaign2nd']) &
                (self.__g_dfPeriodDataRaw['media_camp3rd'] == dict_camp_rst['campaign3rd'])]
            del df_campaign['media_ua']
            del df_campaign['media_brd']
            del df_campaign['media_rst_type']
            del df_campaign['media_source']
            del df_campaign['media_media']
            del df_campaign['media_term']
            del df_campaign['media_camp1st']
            del df_campaign['media_camp2nd']
            del df_campaign['media_camp3rd']
            # no [inplace=True] param to avoid SettingWithCopyWarning of pandas
            df_renamed = df_campaign.rename({'media_gross_cost_inc_vat': 'campaign_gross_cost_inc_vat'}, axis=1)
            del df_campaign
            return df_renamed.sum().to_dict()
        else:
            return {'media_imp': 0, 'media_click': 0, 'in_site_tot_session': 0, 'in_site_tot_bounce': 0,
                    'in_site_tot_duration_sec': 0, 'in_site_tot_pvs': 0, 'in_site_tot_new': 0,
                    'campaign_gross_cost_inc_vat': 0}

    def get_gross_filtered_by_index(self, lst_filter):
        lst_pd_series = []
        for dict_filter in lst_filter:
            lst_pd_series.append(self.__g_dfPeriodDataRaw[dict_filter['col_ttl']] == dict_filter['fil_val'])

        b_first = True
        for pd_series in lst_pd_series:
            if b_first:
                combined_pd_series = pd_series
                b_first = False
            else:
                combined_pd_series = combined_pd_series & pd_series
        del lst_pd_series
        df_filtered = self.__g_dfPeriodDataRaw[combined_pd_series].resample(self.__g_sFreqMode).sum()
        # begin - calculate derivative index
        df_filtered['avg_dur_sec'] = df_filtered['in_site_tot_duration_sec'] / df_filtered['in_site_tot_session']
        df_filtered['avg_dur_sec'].replace(np.nan, 0, inplace=True)
        df_filtered['avg_pvs'] = df_filtered['in_site_tot_pvs'] / df_filtered['in_site_tot_session']
        df_filtered['avg_pvs'].replace(np.nan, 0, inplace=True)
        df_filtered['gross_cpc_inc_vat'] = df_filtered['media_gross_cost_inc_vat'] / df_filtered['media_click']
        df_filtered['gross_cpc_inc_vat'].replace(np.nan, 0, inplace=True)
        df_filtered['gross_cpc_inc_vat'].replace(np.inf, 0, inplace=True)
        df_filtered['gross_ctr'] = df_filtered['media_click'] / df_filtered['media_imp'] * 100
        df_filtered['gross_ctr'].replace(np.nan, 0, inplace=True)
        df_filtered['effective_session'] = df_filtered['in_site_tot_session'] - df_filtered['in_site_tot_bounce']
        # end - calculate derivative index
        return df_filtered

    def get_gross_group_by_index(self, lst_columns):
        # list에 유효 필드만 있는지 검사해야 함
        if type(lst_columns) != list:
            raise Exception('invalid groupby columns')
        df_grp = self.__g_dfPeriodDataRaw.groupby(lst_columns).sum()
        # begin - calculation
        df_grp['avg_dur_sec'] = df_grp['in_site_tot_duration_sec'] / df_grp['in_site_tot_session']
        df_grp['avg_dur_sec'].replace(np.nan, 0, inplace=True)
        df_grp['avg_pvs'] = df_grp['in_site_tot_pvs'] / df_grp['in_site_tot_session']
        df_grp['avg_pvs'].replace(np.nan, 0, inplace=True)
        df_grp['gross_cpc_inc_vat'] = df_grp['media_gross_cost_inc_vat'] / df_grp['media_click']
        df_grp['gross_cpc_inc_vat'].replace(np.nan, 0, inplace=True)
        df_grp['gross_cpc_inc_vat'].replace(np.inf, 0, inplace=True)
        df_grp['gross_ctr'] = df_grp['media_click'] / df_grp['media_imp'] * 100
        df_grp['gross_ctr'].replace(np.nan, 0, inplace=True)
        df_grp['effective_session'] = df_grp['in_site_tot_session'] - df_grp['in_site_tot_bounce']
        # del df_grp['in_site_tot_duration_sec']
        # del df_grp['in_site_tot_pvs']
        # end - calculation
        return df_grp

    def get_full_period_idx(self):
        return self.__g_idxFullPeriod

    def get_full_period_est_factor(self):
        return self.__g_fFullPeriodEstFactor

    def compress_data_by_freq_mode(self):
        """
        set self.__g_dfCompressedByFreqMode
        compress data by designated freq mode
        :return:
        """
        # remove non-quantitative columns
        df_compressed = self.__g_dfPeriodDataRaw.drop(
            ['media_ua', 'media_term', 'media_source', 'media_rst_type', 'media_media', 'media_brd'], axis=1)
        # compress data
        df_compressed = df_compressed.resample(self.__g_sFreqMode).sum()
        # begin - calculate derivative index
        df_compressed.insert(3, 'avg_dur_sec', df_compressed['in_site_tot_duration_sec'] / df_compressed['in_site_tot_session'])
        df_compressed['avg_dur_sec'].replace(np.nan, 0, inplace=True)
        df_compressed.insert(3, 'avg_pvs', df_compressed['in_site_tot_pvs'] / df_compressed['in_site_tot_session'])
        df_compressed['avg_pvs'].replace(np.nan, 0, inplace=True)
        df_compressed.insert(3, 'gross_cpc_inc_vat', df_compressed['media_gross_cost_inc_vat'] / df_compressed['media_click'])
        df_compressed['gross_cpc_inc_vat'].replace(np.nan, 0, inplace=True)
        df_compressed.insert(3, 'gross_ctr', df_compressed['media_click'] / df_compressed['media_imp'] * 100)
        df_compressed['gross_ctr'].replace(np.nan, 0, inplace=True)
        df_compressed.insert(3, 'effective_session', df_compressed['in_site_tot_session'] - df_compressed['in_site_tot_bounce'])
        # end - calculate derivative index
        self.__g_dfCompressedByFreqMode = df_compressed
        del df_compressed

    def aggregate_data(self, dt_mtd_elapsed_days):
        """
        depends on self.__g_dfCompressedByFreqMode
        summarize compressed data
        estimate index if this period
        get accumulate to-date if prev period
        :param dt_mtd_elapsed_days: 당기 기준 MTD 구하기 위한 경과일
        :return:
        """
        if self.__g_fFullPeriodEstFactor == 1:  # MTD mode; completed prev period     on-going current period
            # begin - MTD calculation
            dt_mtd_date = self.__g_dtDesignatedFirstDate + dt_mtd_elapsed_days
            s_mtd_date = dt_mtd_date.strftime("%Y-%m-%d")
            del dt_mtd_date

            try:
                df_aggregation_mtd = self.__g_dfCompressedByFreqMode[
                    (self.__g_dfCompressedByFreqMode.index <= s_mtd_date)].sum()
            except ValueError:  # 당월의 일수가 전월보다 긴 경우, 전체 월 가져오기
                df_aggregation_mtd = self.__g_dfCompressedByFreqMode.sum()

            if df_aggregation_mtd['in_site_tot_duration_sec'] > 0 and df_aggregation_mtd['in_site_tot_session'] > 0:
                df_aggregation_mtd['avg_dur_sec'] = df_aggregation_mtd['in_site_tot_duration_sec'] / \
                                                    df_aggregation_mtd['in_site_tot_session']
            else:
                df_aggregation_mtd['avg_dur_sec'] = 0

            if df_aggregation_mtd['in_site_tot_pvs'] > 0 and df_aggregation_mtd['in_site_tot_session'] > 0:
                df_aggregation_mtd['avg_pvs'] = df_aggregation_mtd['in_site_tot_pvs'] / \
                                                df_aggregation_mtd['in_site_tot_session']
            else:
                df_aggregation_mtd['avg_pvs'] = 0

            if df_aggregation_mtd['media_gross_cost_inc_vat'] > 0 and df_aggregation_mtd['media_click'] > 0:
                df_aggregation_mtd['gross_cpc_inc_vat'] = df_aggregation_mtd['media_gross_cost_inc_vat'] / \
                                                          df_aggregation_mtd['media_click']
            else:
                df_aggregation_mtd['gross_cpc_inc_vat'] = 0

            if df_aggregation_mtd['media_click'] > 0 and df_aggregation_mtd['media_imp'] > 0:
                df_aggregation_mtd['gross_ctr'] = df_aggregation_mtd['media_click'] / \
                                                  df_aggregation_mtd['media_imp'] * 100
            else:
                df_aggregation_mtd['gross_ctr'] = 0
            # end - MTD calculation

            # begin - estimation = full period raw data
            df_aggregation_est = self.__g_dfCompressedByFreqMode.sum()
            if df_aggregation_est['in_site_tot_duration_sec'] > 0 and df_aggregation_est['in_site_tot_session'] > 0:
                df_aggregation_est['avg_dur_sec'] = df_aggregation_est['in_site_tot_duration_sec'] / \
                                                    df_aggregation_est['in_site_tot_session']
            else:
                df_aggregation_est['avg_dur_sec'] = 0

            if df_aggregation_est['in_site_tot_pvs'] > 0 and df_aggregation_est['in_site_tot_session'] > 0:
                df_aggregation_est['avg_pvs'] = df_aggregation_est['in_site_tot_pvs'] / \
                                                df_aggregation_est['in_site_tot_session']
            else:
                df_aggregation_est['avg_pvs'] = 0

            if df_aggregation_est['media_gross_cost_inc_vat'] > 0 and df_aggregation_est['media_click'] > 0:
                df_aggregation_est['gross_cpc_inc_vat'] = df_aggregation_est['media_gross_cost_inc_vat'] / \
                                                          df_aggregation_est['media_click']
            else:
                df_aggregation_est['gross_cpc_inc_vat'] = 0

            if df_aggregation_est['media_click'] > 0 and df_aggregation_est['media_imp'] > 0:
                df_aggregation_est['gross_ctr'] = df_aggregation_est['media_click'] / \
                                                  df_aggregation_est['media_imp'] * 100
            else:
                df_aggregation_est['gross_ctr'] = 0
            # end - estimation = full period raw data

            del df_aggregation_mtd['in_site_tot_duration_sec']
            del df_aggregation_mtd['in_site_tot_pvs']
            self.__g_dictAggregatedDf[self.__g_sClassId]['mtd'] = df_aggregation_mtd.to_dict()
            del df_aggregation_est['in_site_tot_duration_sec']
            del df_aggregation_est['in_site_tot_pvs']
            self.__g_dictAggregatedDf[self.__g_sClassId]['full'] = df_aggregation_est.to_dict()
            del df_aggregation_mtd
            del df_aggregation_est
        else:  # Estimation mode; on-going current period
            # begin - MTD calculation
            df_aggregation_mtd = self.__g_dfCompressedByFreqMode.sum()  # MTD is raw for on-going period
            if df_aggregation_mtd['in_site_tot_duration_sec'] > 0 and df_aggregation_mtd['in_site_tot_session'] > 0:
                df_aggregation_mtd['avg_dur_sec'] = df_aggregation_mtd['in_site_tot_duration_sec'] / \
                                                    df_aggregation_mtd['in_site_tot_session']
            else:
                df_aggregation_mtd['avg_dur_sec'] = 0

            if df_aggregation_mtd['in_site_tot_pvs'] > 0 and df_aggregation_mtd['in_site_tot_session'] > 0:
                df_aggregation_mtd['avg_pvs'] = df_aggregation_mtd['in_site_tot_pvs'] / \
                                                df_aggregation_mtd['in_site_tot_session']
            else:
                df_aggregation_mtd['avg_pvs'] = 0

            if df_aggregation_mtd['media_gross_cost_inc_vat'] > 0 and df_aggregation_mtd['media_click'] > 0:
                df_aggregation_mtd['gross_cpc_inc_vat'] = df_aggregation_mtd['media_gross_cost_inc_vat'] / \
                                                          df_aggregation_mtd['media_click']
            else:
                df_aggregation_mtd['gross_cpc_inc_vat'] = 0

            if df_aggregation_mtd['media_click'] > 0 and df_aggregation_mtd['media_imp'] > 0:
                df_aggregation_mtd['gross_ctr'] = df_aggregation_mtd['media_click'] / \
                                                  df_aggregation_mtd['media_imp'] * 100
            else:
                df_aggregation_mtd['gross_ctr'] = 0
            # end - MTD calculation

            # begin - construct memory for estimation
            dict_est = {'media_imp': int(df_aggregation_mtd['media_imp'] * self.__g_fFullPeriodEstFactor),
                        'media_click': int(df_aggregation_mtd['media_click'] * self.__g_fFullPeriodEstFactor),
                        'in_site_tot_session': int(
                            df_aggregation_mtd['in_site_tot_session'] * self.__g_fFullPeriodEstFactor),
                        'in_site_tot_bounce': int(
                            df_aggregation_mtd['in_site_tot_bounce'] * self.__g_fFullPeriodEstFactor),
                        'in_site_tot_new': int(df_aggregation_mtd['in_site_tot_new'] * self.__g_fFullPeriodEstFactor),
                        'media_gross_cost_inc_vat': int(
                            df_aggregation_mtd['media_gross_cost_inc_vat'] * self.__g_fFullPeriodEstFactor),
                        'effective_session': int(
                            df_aggregation_mtd['effective_session'] * self.__g_fFullPeriodEstFactor),
                        'avg_dur_sec': df_aggregation_mtd['avg_dur_sec'],
                        'avg_pvs': df_aggregation_mtd['avg_pvs'],
                        'gross_cpc_inc_vat': df_aggregation_mtd['gross_cpc_inc_vat'],
                        'gross_ctr': df_aggregation_mtd['gross_ctr']}
            # end - construct memory for estimation
            del df_aggregation_mtd['in_site_tot_duration_sec']
            del df_aggregation_mtd['in_site_tot_pvs']
            self.__g_dictAggregatedDf[self.__g_sClassId]['mtd'] = df_aggregation_mtd.to_dict()
            self.__g_dictAggregatedDf[self.__g_sClassId]['full'] = dict_est
            del df_aggregation_mtd

    def __construct_dataframe(self):
        # begin - get period range index based on frequency mode
        dt_last_date_of_month = self.__get_last_day_of_month(self.__g_dtDesignatedFirstDate)
        self.__g_idxFullPeriod = pd.date_range(start=self.__g_dtDesignatedFirstDate, end=dt_last_date_of_month,
                                               freq=self.__g_sFreqMode)
        idx_full_day = self.__g_idxFullPeriod
        lst_raw_data = self.__g_oSvDb.executeQuery('getGaMediaDailyLogByBrandId', self.__g_dtDesignatedFirstDate,
                                                   self.__g_dtDesignatedLastDate)
        
        if lst_raw_data and 'err_code' in lst_raw_data.pop().keys():  # for an initial stage; no table
            lst_raw_data = []

        lst_blank_raw = []
        if len(lst_raw_data) == 0:  # set blank dataset if no period data
            for pd_datetime in idx_full_day:
                lst_blank_raw.append({'media_ua': 'P', 'media_rst_type': 'NS', 'media_source': '(direct)',
                                      'media_media': '(none)', 'media_raw_cost': 0, 'media_agency_cost': 0,
                                      'media_cost_vat': 0, 'media_imp': 0, 'media_click': 0, 'media_term': '(notset)',
                                      'media_brd': 0, 'in_site_tot_session': 0, 'in_site_tot_bounce': 0,
                                      'in_site_tot_duration_sec': 0, 'in_site_tot_pvs': 0, 'in_site_tot_new': 0,
                                      'in_site_revenue': 0, 'in_site_trs': 0,
                                      'media_camp1st': '', 'media_camp2nd': '', 'media_camp3rd': '',
                                      'logdate': pd_datetime})
            self.__g_dfPeriodDataRaw = pd.DataFrame(lst_blank_raw)
        else:  # if period data
            self.__g_dfPeriodDataRaw = pd.DataFrame(lst_raw_data)
            # ensure logdate field to datetime format
            self.__g_dfPeriodDataRaw['logdate'] = pd.to_datetime(self.__g_dfPeriodDataRaw['logdate'])

            # begin - calculate weight factor for estimation
            lst_raw_data_date = self.__g_dfPeriodDataRaw['logdate'].unique().tolist()
            n_raw_data_first_date = lst_raw_data_date[0]  # 추출 데이터 기준 기간 시작일
            n_raw_data_last_date = lst_raw_data_date[-1]  # 추출 데이터 기준 기간 마지막일
            del lst_raw_data_date
            idx_raw_data_period = pd.date_range(start=n_raw_data_first_date, end=n_raw_data_last_date,
                                                freq=self.__g_sFreqMode)
            n_full_day_cnt = len(idx_full_day)
            n_actual_day_cnt = len(idx_raw_data_period)
            del idx_raw_data_period
            if n_full_day_cnt == n_actual_day_cnt:  # 청구일이 청구월의 마지막 날이면 예상 가중치는 1
                pass  # self.__g_fFullPeriodEstFactor = 1
            elif n_actual_day_cnt < n_full_day_cnt:  # 청구일이 청구월의 마지막 날보다 과거면 예상 가중치 계산
                dt_yesterday = datetime.today() - timedelta(1)
                self.__g_fFullPeriodEstFactor = 1 / dt_yesterday.day * dt_last_date_of_month.day
                del dt_yesterday
            # end - calculate weight factor for estimation

            # begin - 사용자 요청 기간보다 데이터 기간이 짧으면 누락일을 채움
            lst_index = []
            for pd_datetime in idx_full_day:
                if pd_datetime not in self.__g_dfPeriodDataRaw['logdate'].values:
                    lst_blank_raw.append({'media_ua': 'P', 'media_rst_type': 'NS', 'media_source': '(direct)',
                                          'media_media': '(none)', 'media_raw_cost': 0, 'media_agency_cost': 0,
                                          'media_cost_vat': 0, 'media_imp': 0, 'media_click': 0,
                                          'media_term': '(notset)',
                                          'media_brd': 0, 'in_site_tot_session': 0, 'in_site_tot_bounce': 0,
                                          'in_site_tot_duration_sec': 0, 'in_site_tot_pvs': 0, 'in_site_tot_new': 0,
                                          'in_site_revenue': 0, 'in_site_trs': 0,
                                          'media_camp1st': '', 'media_camp2nd': '', 'media_camp3rd': '',
                                          'logdate': pd_datetime})
                    lst_index.append(pd_datetime)
            if len(lst_index) > 0:
                df_null_date = pd.DataFrame(lst_blank_raw, index=lst_index)
                self.__g_dfPeriodDataRaw = pd.concat([self.__g_dfPeriodDataRaw, df_null_date])
            del lst_index
            # end - 사용자 요청 기간보다 데이터 기간이 짧으면 누락일을 0으로 채움
        del lst_blank_raw
        del idx_full_day
        self.__g_dfPeriodDataRaw = self.__g_dfPeriodDataRaw.set_index('logdate')
        self.__g_dfPeriodDataRaw['media_gross_cost_inc_vat'] = self.__g_dfPeriodDataRaw['media_raw_cost'] + \
                                                               self.__g_dfPeriodDataRaw['media_agency_cost'] + \
                                                               self.__g_dfPeriodDataRaw['media_cost_vat']
        # begin - retrieve agency fee
        df_by_source_medium = self.__g_dfPeriodDataRaw.groupby(['media_source', 'media_media']).sum()
        df = df_by_source_medium.sort_values(['media_gross_cost_inc_vat'], ascending=False)
        for i, df_row in df.iterrows():
            if df_row['media_agency_cost'] > 0:
                n_agency_fee_inc_vat = df_row['media_agency_cost'] * 1.1  # add VAT  #
                self.__g_dictBudgetInfo['_'.join(df_row.name)] = {
                    'n_budget_tgt_amnt_inc_vat': 0,
                    'n_agency_fee_inc_vat': n_agency_fee_inc_vat,
                    'n_agency_fee_inc_vat_est': 0,
                    'f_est_factor': 1,
                    'b_campaign_level': False
                }
        # end - retrieve agency fee
        # begin - retrieve planned budget
        from .budget import Budget
        o_budget = Budget(self.__g_oSvDb)
        dict_budget = o_budget.get_budget_amnt_by_period(dt_start=self.__g_dtDesignatedFirstDate,
                                                         dt_end=dt_last_date_of_month)
        del o_budget
        for s_source_medium, dict_budget in dict_budget.items():
            if s_source_medium in self.__g_dictBudgetInfo:  #.keys():  # update existing
                self.__g_dictBudgetInfo[s_source_medium]['n_budget_tgt_amnt_inc_vat'] = \
                    dict_budget['n_budget_tgt_amnt_inc_vat']
                f_est_factor = self.__get_estimate_factor(dict_budget['dt_period_start'], dict_budget['dt_period_end'])
                if f_est_factor >= 1:  # f_est_factor == 1 means budget schedule has been finished
                    self.__g_dictBudgetInfo[s_source_medium]['n_agency_fee_inc_vat_est'] = \
                        self.__g_dictBudgetInfo[s_source_medium]['n_agency_fee_inc_vat'] * f_est_factor
                self.__g_dictBudgetInfo[s_source_medium]['f_est_factor'] = f_est_factor
            else:  # add new
                self.__g_dictBudgetInfo[s_source_medium] = {
                    'n_budget_tgt_amnt_inc_vat': dict_budget['n_budget_tgt_amnt_inc_vat'],
                    'n_agency_fee_inc_vat': 0,
                    'n_agency_fee_inc_vat_est': 0,
                    'f_est_factor': 1,
                    'b_campaign_level': False
                }
            # begin - arrange campaign level budget if exists
            if dict_budget['b_campaign_level']:
                self.__g_dictBudgetInfo[s_source_medium]['b_campaign_level'] = True
                self.__g_dictBudgetInfo[s_source_medium]['dict_campaign'] = {}
                for s_camp_title, dict_single_campaign in dict_budget['dict_campaign'].items():
                    n_budget_tgt_amnt_inc_vat = dict_single_campaign['n_budget_tgt_amnt_inc_vat']
                    f_est_factor = self.__get_estimate_factor(dict_single_campaign['dt_period_start'],
                                                              dict_single_campaign['dt_period_end'])
                    self.__g_dictBudgetInfo[s_source_medium]['dict_campaign'][s_camp_title] = \
                        {'n_budget_tgt_amnt_inc_vat': n_budget_tgt_amnt_inc_vat, 'f_est_factor': f_est_factor}
            # end - arrange campaign level budget if exists
        # end - retrieve planned budget
        # remove unnecessary columns to minimize DF sum() overload
        del self.__g_dfPeriodDataRaw['media_raw_cost']
        del self.__g_dfPeriodDataRaw['media_agency_cost']
        del self.__g_dfPeriodDataRaw['media_cost_vat']

        # remove GA ecommerce data if null
        if self.__g_dfPeriodDataRaw['in_site_revenue'].sum() == 0:
            del self.__g_dfPeriodDataRaw['in_site_revenue']
        if self.__g_dfPeriodDataRaw['in_site_trs'].sum() == 0:
            del self.__g_dfPeriodDataRaw['in_site_trs']

    def __get_estimate_factor(self, dt_start, dt_end):
        if dt_end < self.__g_dtToday:  # budget completed
            f_est_factor = 1
        else:  # estimating budget
            if dt_start >= self.__g_dtToday:  # no estimation if a upcoming budget
                f_est_factor = 0
            else:
                idx_budget_entire_period = pd.date_range(start=dt_start, end=dt_end, freq=self.__g_sFreqMode)
                n_budget_entire_period_day_cnt = len(idx_budget_entire_period)
                del idx_budget_entire_period
                idx_budget_elapsed_period = pd.date_range(start=dt_start, end=self.__g_dtToday, freq=self.__g_sFreqMode)
                n_budget_elapsed_period_day_cnt = \
                    len(idx_budget_elapsed_period) - 1  # must be not today but yesterday
                del idx_budget_elapsed_period
                f_est_factor = 1 / n_budget_elapsed_period_day_cnt * n_budget_entire_period_day_cnt
        return f_est_factor

    def __get_last_day_of_month(self, dt_point):
        if dt_point.month == 12:
            return dt_point.replace(day=31)
        return dt_point.replace(month=dt_point.month + 1, day=1) - relativedelta(days=1)
