# -*- coding: UTF-8 -*-
# UTF-8 테스트

# Copyright 2021 singleview.co.kr, Inc.

# You are hereby granted a non-exclusive, worldwide, royalty-free license to
# use, copy, modify, and distribute this software in source code or binary
# form for use in connection with the web services and APIs provided by
# singleview.co.kr.

# As with any software that integrates with the singleview.co.kr platform, 
# your use of this software is subject to the Facebook Developer Principles 
# and Policies [http://singleview.co.kr/api_policy/]. This copyright 
# notice shall be included in all copies or substantial portions of the 
# software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

# refer to https://github.com/google/google-api-python-client/tree/master/samples/analytics
# you firstly need to install by cmd "pip3.6 install --upgrade google-api-python-client"
# refer to https://developers.google.com/analytics/devguides/config/mgmt/v3/quickstart/installed-py
# to create desinated credential refer to https://console.developers.google.com/apis/credentials
# to get console credential you firstly need to run with the option --noauth_local_webserver 
# to monitor API traffic refer to https://console.developers.google.com/apis/api/analytics.googleapis.com/quotas?project=svgastudio

# standard library
import logging
from datetime import date
from datetime import datetime
from datetime import timedelta
import sys
import os
import calendar

# 3rd party library

# singleview library
if __name__ == '__main__':  # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svload/pandas_plugins')
    sys.path.append('../../svdjango')
    import sv_mysql
    import sv_campaign_parser
    import sv_agency_info
    import sv_object
    import sv_plugin
    import settings
else:  # for platform running
    from svcommon import sv_mysql
    from svcommon import sv_campaign_parser
    from svcommon import sv_agency_info
    from svcommon import sv_object
    from svcommon import sv_plugin
    from django.conf import settings

SEPARATOR = '|@|'


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_oSvCampaignParser = sv_campaign_parser.SvCampaignParser()
    # __g_nPnsTouchingDate = 20190126  # to separate the old & non-systematic & complicated situation for PNS cost process
    __g_dictSourceTitleTag = None
    # __g_dictNvPnsUaCostPortion = {'M': 0.7, 'P': 0.3}  # sum must be 1
    __g_sSvNull = '#%'

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name+'(20230605)')
        
        self._g_dictParam.update({'yyyymm': None, 'mode': None})
        # Declaring a dict outside __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        self.__g_sSvAcctId = None
        self.__g_sBrandId = None
        self.__g_sMode = None
        self.__g_bFbProcess = False
        self.__g_bGoogleAdsProcess = False
        self.__g_sDataPath = None
        self.__g_sRetrieveMonth = None
        self.__g_oSvMysql = None
        self.__g_sDate = None
        self.__g_dictNvadMergedDailyLog = {}
        self.__g_dictAdwMergedDailyLog = {}
        self.__g_dictKkoMergedDailyLog = {}
        self.__g_dictYtMergedDailyLog = {}
        self.__g_dictFbMergedDailyLog = {}
        self.__g_dictOtherMergedDailyLog = {}
        self.__g_oAgencyInfo = None

    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        self.__g_sSvAcctId = None
        self.__g_sBrandId = None
        self.__g_sMode = None
        self.__g_bFbProcess = False
        self.__g_bGoogleAdsProcess = False
        self.__g_sDataPath = None
        self.__g_sRetrieveMonth = None
        self.__g_oSvMysql = None
        self.__g_sDate = None
        self.__g_dictNvadMergedDailyLog.clear()
        self.__g_dictAdwMergedDailyLog.clear()
        self.__g_dictKkoMergedDailyLog.clear()
        self.__g_dictYtMergedDailyLog.clear()
        self.__g_dictFbMergedDailyLog.clear()
        self.__g_dictOtherMergedDailyLog.clear()
        self.__g_oAgencyInfo = None

    def do_task(self, o_callback):
        self._g_oCallback = o_callback
        self.__g_sRetrieveMonth = self._g_dictParam['yyyymm']
        self.__g_sMode = self._g_dictParam['mode']

        dict_acct_info = self._task_pre_proc(o_callback)
        if 'sv_account_id' not in dict_acct_info and 'brand_id' not in dict_acct_info and \
                'nvr_ad_acct' not in dict_acct_info:
            self._print_debug('stop -> invalid config_loc')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        self.__g_sSvAcctId = dict_acct_info['sv_account_id']
        self.__g_sBrandId = dict_acct_info['brand_id']
        self.__g_sTblPrefix = dict_acct_info['tbl_prefix']
        self.__g_sDataPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, self.__g_sSvAcctId,
                                          self.__g_sBrandId)

        if self.__g_dictSourceTitleTag is None:
            self.__g_dictSourceTitleTag = self.__g_oSvCampaignParser.get_source_tag_title_dict(b_inverted=True)
        
        with sv_mysql.SvMySql() as oSvMysql:
            oSvMysql.set_tbl_prefix(self.__g_sTblPrefix)
            oSvMysql.set_app_name('svplugins.integrate_db')
            oSvMysql.initialize(self._g_dictSvAcctInfo)
        
        self.__g_oAgencyInfo = sv_agency_info.SvAgencyInfo()
        if self.__g_sMode == 'clear':
            self.__truncate_compiled_tbl()
            self._task_post_proc(self._g_oCallback)
            return
        elif self.__g_sRetrieveMonth is not None:
            dict_date_range = self.__delete_specific_month()
        else:
            dict_date_range = self.__get_touch_date_range()

        if dict_date_range['start_date'] is None and dict_date_range['end_date'] is None:
            self._print_debug('stop - weird raw data last date')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return

        dt_start = dict_date_range['start_date']
        dt_end = dict_date_range['end_date']
        if dt_start > dt_end:
            self._print_debug('error - weird raw data last date')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        date_generated = [dt_start + timedelta(days=x) for x in range(0, (dt_end - dt_start).days + 1)]
        n_idx = 0
        n_sentinel = len(date_generated)
        for dt_date in date_generated:
            if not self._continue_iteration():
                break
            self.__g_sDate = dt_date.strftime('%Y-%m-%d')
            self.__compile_daily_record()
            self._print_progress_bar(n_idx + 1, n_sentinel, prefix='Arrange data:', suffix='Complete', length=50)
            n_idx += 1

        self._task_post_proc(self._g_oCallback)

    def __truncate_compiled_tbl(self):
        self._print_debug('-> clear ga media daily compiled log')
        with sv_mysql.SvMySql() as oSvMysql:
            oSvMysql.set_tbl_prefix(self.__g_sTblPrefix)
            oSvMysql.set_app_name('svplugins.integrate_db')
            oSvMysql.initialize(self._g_dictSvAcctInfo)
            oSvMysql.truncate_tbl('compiled_ga_media_daily_log')

    def __delete_specific_month(self):
        dict_rst = {'start_date': None, 'end_date': None}
        n_yr = int(self.__g_sRetrieveMonth[:4])
        n_mo = int(self.__g_sRetrieveMonth[4:None])
        try:
            lst_month_range = calendar.monthrange(n_yr, n_mo)
        except calendar.IllegalMonthError:
            self._print_debug('invalid yyyymm')
            return dict_rst

        # check google-ads API directory exists
        if os.path.exists(os.path.join(self.__g_sDataPath, 'adwords')):
            self.__g_bGoogleAdsProcess = True

        # check fb biz API directory exists
        s_fb_biz_abs_path = os.path.join(self.__g_sDataPath, 'fb_biz')
        if os.path.exists(s_fb_biz_abs_path):
            lst_directory = os.listdir(s_fb_biz_abs_path)
            for s_cid in lst_directory:
                if s_cid == 'alias_info_campaign.tsv':
                    continue
                if os.path.exists(os.path.join(s_fb_biz_abs_path, s_cid, 'conf', 'general.latest')):
                    self.__g_bFbProcess = True
                    break
            del lst_directory
        s_start_date_retrieval = self.__g_sRetrieveMonth[:4] + '-' + self.__g_sRetrieveMonth[4:None] + '-01'
        s_end_date_retrieval = self.__g_sRetrieveMonth[:4] + '-' + self.__g_sRetrieveMonth[4:None] + '-' + str(
            lst_month_range[1])
        with sv_mysql.SvMySql() as oSvMysql:
            oSvMysql.set_tbl_prefix(self.__g_sTblPrefix)
            oSvMysql.set_app_name('svplugins.integrate_db')
            oSvMysql.initialize(self._g_dictSvAcctInfo)
            oSvMysql.execute_query('deleteCompiledGaMediaLogByPeriod', s_start_date_retrieval, s_end_date_retrieval)
        dict_rst['start_date'] = datetime.strptime(s_start_date_retrieval, '%Y-%m-%d')
        dict_rst['end_date'] = datetime.strptime(s_end_date_retrieval, '%Y-%m-%d')
        return dict_rst

    def __get_touch_date_range(self):
        """ get latest retrieval info """
        dict_rst = {'start_date': None, 'end_date': None}
        dt_today = date.today()
        dt_yesterday = dt_today - timedelta(1)
        del dt_today

        s_naver_ad_api_root = os.path.join(self.__g_sDataPath, 'naver_ad')
        if os.path.exists(s_naver_ad_api_root):
            lst_directory = os.listdir(s_naver_ad_api_root)
            for s_ignore in ['alias_info_adgrp.tsv', 'pns']:
                if s_ignore in lst_directory:
                    lst_directory.remove(s_ignore)
        lst_nvrad_last_date = []
        for s_cid in lst_directory:
            # check naver_ad latest retrieved date
            if not self.__g_oAgencyInfo.load_by_source_id(self.__g_sSvAcctId, self.__g_sBrandId, 'naver_ad', s_cid):
                return dict_rst
            o_file = open(os.path.join(s_naver_ad_api_root, s_cid, 'conf', 'AD.latest'), 'r')
            s_latest_date = o_file.read()
            o_file.close
            # dtLatest = date(int(sLatestDate[:4]), int(sLatestDate[4:6]), int(sLatestDate[6:8]))
            # lst_nvrad_last_date.append(dtLatest)
            lst_nvrad_last_date.append(
                date(int(s_latest_date[:4]), int(s_latest_date[4:6]), int(s_latest_date[6:8])))
        del lst_directory
        if len(lst_nvrad_last_date) == 0:
            lst_nvrad_last_date.append(dt_yesterday)

        # get latest retrieval info of google ads
        s_googlead_api_root = os.path.join(self.__g_sDataPath, 'adwords')
        if os.path.exists(s_googlead_api_root):
            self.__g_bGoogleAdsProcess = True
            lst_directory = os.listdir(s_googlead_api_root)
            for s_ignore in ['alias_info_campaign.tsv']:
                if s_ignore in lst_directory:
                    lst_directory.remove(s_ignore)
        # lst_googleads_last_date = []
        # if self.__g_bGoogleAdsProcess:
        #     for s_cid in lst_directory:
        #         # check google-ads latest retrieved date
        #         if not self.__g_oAgencyInfo.load_by_source_id(self.__g_sSvAcctId, self.__g_sBrandId, 'adwords', s_cid):
        #             return dict_rst
        #         s_googlead_latest_file = os.path.join(s_googlead_api_root, s_cid, 'conf', 'general.latest')
        #         if os.path.exists(s_googlead_latest_file):
        #             o_file = open(s_googlead_latest_file, 'r')
        #             s_latest_date = o_file.read()
        #             o_file.close()
        #         else:
        #             s_latest_date = dt_yesterday.strftime("%Y%m%d")
        #         print(s_latest_date)
        #         lst_googleads_last_date.append(
        #             date(int(s_latest_date[:4]), int(s_latest_date[4:6]), int(s_latest_date[6:8])))
        # del lst_directory

        # get a latest retrieval info of fb_biz
        b_fb_biz_proc = True
        s_fb_biz_api_root = os.path.join(self.__g_sDataPath, 'fb_biz')
        if os.path.exists(s_fb_biz_api_root):
            lst_directory = os.listdir(s_fb_biz_api_root)
            for s_ignore in ['alias_info_campaign.tsv']:
                if s_ignore in lst_directory:
                    lst_directory.remove(s_ignore)
        else:
            lst_directory = []
            b_fb_biz_proc = False
            self.__g_bFbProcess = False
        lst_fbbiz_last_date = []
        if b_fb_biz_proc:
            for s_cid in lst_directory:
                # check agency_info.tsv
                if not self.__g_oAgencyInfo.load_by_source_id(self.__g_sSvAcctId, self.__g_sBrandId, 'fb_biz', s_cid):
                    return dict_rst
                s_fb_biz_latest_file = os.path.join(s_fb_biz_api_root, s_cid, 'conf', 'general.latest')
                if os.path.exists(s_fb_biz_latest_file):
                    o_file = open(s_fb_biz_latest_file, 'r')
                    s_latest_date = o_file.read()
                    o_file.close()
                    lst_fbbiz_last_date.append(
                        date(int(s_latest_date[:4]), int(s_latest_date[4:6]), int(s_latest_date[6:8])))
                    self.__g_bFbProcess = True
        del lst_directory
        if self.__g_sMode == 'ignore_fb':
            self.__g_bFbProcess = False

        with sv_mysql.SvMySql() as oSvMysql:  # to enforce follow strict mysql connection mgmt
            oSvMysql.set_tbl_prefix(self.__g_sTblPrefix)
            oSvMysql.set_app_name('svplugins.integrate_db')
            oSvMysql.initialize(self._g_dictSvAcctInfo)
            # define last date of process
            lst_last_date = []
            lst_ga_log_date_range = oSvMysql.execute_query('getGaLogDateMaxMin')
            lst_last_date.append(lst_ga_log_date_range[0]['maxdate'])
            # if self.__g_bGoogleAdsProcess:
            #     lst_last_date.append(max(lst_googleads_last_date))

            lst_last_date.append(max(lst_nvrad_last_date))
            dict_rst['end_date'] = min(lst_last_date)
            # define first date of process
            lst_first_date = []
            lst_gross_compiled_log_date_range = oSvMysql.execute_query('getCompiledGaMediaLogDateMaxMin')
            if lst_gross_compiled_log_date_range[0]['maxdate'] is None:
                self._print_debug('zero base')
                # decide that GA data period is a population on 20190216
                lst_first_date.append(lst_ga_log_date_range[0]['mindate'])
                dict_rst['start_date'] = max(lst_first_date)
            else:
                dict_rst['start_date'] = lst_gross_compiled_log_date_range[0]['maxdate'] + timedelta(days=1)
        return dict_rst

    def __compile_daily_record(self):
        try:  # validate requested date
            datetime.strptime(self.__g_sDate, '%Y-%m-%d').strftime('%Y-%m-%d')
        except ValueError:
            self._print_debug(self.__g_sDate + ' is invalid date string')
            return
        # clear log
        self.__g_dictNvadMergedDailyLog.clear()
        self.__g_dictAdwMergedDailyLog.clear()
        self.__g_dictKkoMergedDailyLog.clear()
        self.__g_dictYtMergedDailyLog.clear()
        self.__g_dictFbMergedDailyLog.clear()
        self.__g_dictOtherMergedDailyLog.clear()
        # connect to database to register
        self.__g_oSvMysql = sv_mysql.SvMySql()
        self.__g_oSvMysql.set_tbl_prefix(self.__g_sTblPrefix)
        self.__g_oSvMysql.set_app_name('svplugins.integrate_db')
        self.__g_oSvMysql.initialize(self._g_dictSvAcctInfo)
        lst_ga_log_daily = self.__g_oSvMysql.execute_query('getGaLogDaily', self.__g_sDate)
        # if self.__g_sDate == '2019-01-27':
        for lst_ga_log in lst_ga_log_daily:
            if lst_ga_log['source'] == 'naver' and lst_ga_log['rst_type'] == 'PS':
                self.__cleanup_nvad_log(lst_ga_log)
            elif self.__g_bGoogleAdsProcess and lst_ga_log['source'] == 'google' and lst_ga_log['rst_type'] == 'PS':
                self.__cleanup_googleads_log(lst_ga_log)
            elif self.__g_bGoogleAdsProcess and lst_ga_log['source'] == 'youtube' and lst_ga_log['rst_type'] == 'PS':
                self.__cleanup_yt_log(lst_ga_log)
            elif lst_ga_log['source'] == 'kakao' and lst_ga_log['rst_type'] == 'PS':
                self.__cleanup_kko_log(lst_ga_log)
            elif self.__g_bFbProcess and lst_ga_log['source'] == 'facebook' or lst_ga_log['source'] == 'instagram':
                if lst_ga_log['rst_type'] == 'PS':
                    self.__cleanup_fb_log(lst_ga_log)
            else:  # others
                self.__cleanup_other_log(lst_ga_log)

        if len(self.__g_dictNvadMergedDailyLog) > 0:  # only if nvad log exists
            self.__merge_nvad_log()
        if len(self.__g_dictAdwMergedDailyLog) > 0:  # only if google ads log exists
            self.__merge_googleads_log()
        if len(self.__g_dictYtMergedDailyLog) > 0:  # only if YT ads exists
            self.__merge_yt_log()
        else:  # YT out-lading ads without any in-bound session case
            lst_yt_log_daily = self.__g_oSvMysql.execute_query('getAdwLogDaily',
                                                               self.__g_sDate, self.__g_dictSourceTitleTag['youtube'])
            if len(lst_yt_log_daily):
                self.__merge_yt_log()
            del lst_yt_log_daily
        if len(self.__g_dictFbMergedDailyLog) > 0:  # only if facebook business log exists
            self.__merge_fb_log()
        if len(self.__g_dictKkoMergedDailyLog) > 0:  # only if kakao moment log exists
            self.__merge_kko_log()
        self.__merge_other_log()
        self.__g_oSvMysql.disconnect()

    def __convert_daily_log(self, lst_ga_log):
        # s_term = lst_ga_log['term'].replace(' ', '').upper()
        if lst_ga_log['media'] == 'cpc' and lst_ga_log['campaign_1st'].find('GDN') > -1:
            lst_ga_log['term'] = lst_ga_log['campaign_1st'].lower()
            # GA starts to allocate term on session via GDN; some of terms are branded; this corrupts GDN aggregation logic
            lst_ga_log['brd'] = 0
        s_uniq_log_id = lst_ga_log['ua'] + SEPARATOR + lst_ga_log['source'] + SEPARATOR + lst_ga_log['rst_type'] + \
                        SEPARATOR + lst_ga_log['media'] + SEPARATOR + str(lst_ga_log['brd']) + SEPARATOR + \
                        lst_ga_log['campaign_1st'] + SEPARATOR + lst_ga_log['campaign_2nd'] + SEPARATOR + \
                        lst_ga_log['campaign_3rd'] + SEPARATOR + lst_ga_log['term']
        n_session = int(lst_ga_log['session'])
        return {
            's_uniq_log_id': s_uniq_log_id,
            'n_session': n_session,
            'n_trs': int(lst_ga_log['transactions']),
            'n_rev': int(lst_ga_log['revenue']),
            'n_reg': int(lst_ga_log['registrations']),
            'f_gross_new': n_session * float(lst_ga_log['new_per']) / 100,  # f_new_per
            'f_gross_b': n_session * float(lst_ga_log['bounce_per']) / 100,  # f_b_per
            'f_gross_dur_sec': n_session * float(lst_ga_log['duration_sec']),  # f_dur_sec
            'f_gross_pvs': n_session * float(lst_ga_log['pvs'])  # f_pvs
        }

    def __parse_uniq_log_id(self, s_uniq_log_id):
        a_uniq_id = s_uniq_log_id.split(SEPARATOR)
        return {
            's_ua': a_uniq_id[0], 's_source': a_uniq_id[1], 's_rst_type': a_uniq_id[2], 's_media': a_uniq_id[3],
            's_brd': a_uniq_id[4], 's_camp_1st': a_uniq_id[5], 's_camp_2nd': a_uniq_id[6], 's_camp_3rd': a_uniq_id[7],
            's_term': a_uniq_id[8]  # if len(a_uniq_id) == 9 else ''
        }

    def __cleanup_fb_log(self, lst_ga_log):
        # facebook never provides term via their insight API
        # but sometimes set utm_term on their campaign, then GA report term-level specified FB campaign
        # that's why GA term needs to be capitalized before integrate with FB log
        lst_ga_log['term'] = lst_ga_log['term'].replace(' ', '').upper()
        dict_rst = self.__convert_daily_log(lst_ga_log)
        s_uniq_log_id = dict_rst['s_uniq_log_id']
        if self.__g_dictFbMergedDailyLog.get(s_uniq_log_id, self.__g_sSvNull) != self.__g_sSvNull:  # if sRowId exists
            self.__g_dictFbMergedDailyLog[s_uniq_log_id]['session'] += dict_rst['n_session']  # n_session
            self.__g_dictFbMergedDailyLog[s_uniq_log_id]['tot_new_session'] += dict_rst['f_gross_new']  # f_gross_new
            self.__g_dictFbMergedDailyLog[s_uniq_log_id]['tot_bounce'] += dict_rst['f_gross_b']  # f_gross_b
            self.__g_dictFbMergedDailyLog[s_uniq_log_id]['tot_duration_sec'] += dict_rst['f_gross_dur_sec']  # f_gross_dur_sec
            self.__g_dictFbMergedDailyLog[s_uniq_log_id]['tot_pvs'] += dict_rst['f_gross_pvs']  # f_gross_pvs
            self.__g_dictFbMergedDailyLog[s_uniq_log_id]['tot_transactions'] += dict_rst['n_trs']  # n_trs
            self.__g_dictFbMergedDailyLog[s_uniq_log_id]['tot_revenue'] += dict_rst['n_rev']  # n_rev
            self.__g_dictFbMergedDailyLog[s_uniq_log_id]['tot_registrations'] += dict_rst['n_reg']  # n_reg
        else:  # if new log requested
            self.__g_dictFbMergedDailyLog[s_uniq_log_id] = {
                'session': dict_rst['n_session'],  # n_session,
                'tot_new_session': dict_rst['f_gross_new'],  # f_gross_new,
                'tot_bounce': dict_rst['f_gross_b'],  # f_gross_b,
                'tot_duration_sec': dict_rst['f_gross_dur_sec'],  # f_gross_dur_sec,
                'tot_pvs': dict_rst['f_gross_pvs'],  # f_gross_pvs,
                'tot_transactions': dict_rst['n_trs'],  # n_trs,
                'tot_revenue': dict_rst['n_rev'],  # n_rev,
                'tot_registrations': dict_rst['n_reg']  # n_reg
            }

    def __cleanup_googleads_log(self, lst_ga_log):
        # google-ads sometimes provides log like "campaign code = (not set) term = (not set)"
        lst_ga_log['term'] = lst_ga_log['term'].replace(' ', '').upper()
        # upper() needs to be done as ADW capitalizes term always but GA term is case-sensitive.
        # that's why GA term needs to be capitalized before integrate with ADW log
        # merge and create new performance row for GDN
        if lst_ga_log['media'] == 'cpc' and lst_ga_log['campaign_1st'].find('GDN') > -1:
            lst_ga_log['term'] = lst_ga_log['campaign_1st'].lower()
            # GA starts to allocate term on session via GDN; some of terms are branded; this corrupts GDN aggregation logic
            lst_ga_log['brd'] = 0

        dict_rst = self.__convert_daily_log(lst_ga_log)
        s_uniq_log_id = dict_rst['s_uniq_log_id']
        if self.__g_dictAdwMergedDailyLog.get(s_uniq_log_id, self.__g_sSvNull) != self.__g_sSvNull:  # if sRowId exists
            self.__g_dictAdwMergedDailyLog[s_uniq_log_id]['session'] += dict_rst['n_session']  # n_session
            self.__g_dictAdwMergedDailyLog[s_uniq_log_id]['tot_new_session'] += dict_rst['f_gross_new']  # f_gross_new
            self.__g_dictAdwMergedDailyLog[s_uniq_log_id]['tot_bounce'] += dict_rst['f_gross_b']  # f_gross_b
            self.__g_dictAdwMergedDailyLog[s_uniq_log_id]['tot_duration_sec'] += dict_rst['f_gross_dur_sec']  # f_gross_dur_sec
            self.__g_dictAdwMergedDailyLog[s_uniq_log_id]['tot_pvs'] += dict_rst['f_gross_pvs']  # f_gross_pvs
            self.__g_dictAdwMergedDailyLog[s_uniq_log_id]['tot_transactions'] += dict_rst['n_trs']  # n_trs
            self.__g_dictAdwMergedDailyLog[s_uniq_log_id]['tot_revenue'] += dict_rst['n_rev']  # n_rev
            self.__g_dictAdwMergedDailyLog[s_uniq_log_id]['tot_registrations'] += dict_rst['n_reg']  # n_reg
        else:  # if new log requested
            self.__g_dictAdwMergedDailyLog[s_uniq_log_id] = {
                'session': dict_rst['n_session'],  # n_session,
                'tot_new_session': dict_rst['f_gross_new'],  # f_gross_new,
                'tot_bounce': dict_rst['f_gross_b'],  # f_gross_b,
                'tot_duration_sec': dict_rst['f_gross_dur_sec'],  # f_gross_dur_sec,
                'tot_pvs': dict_rst['f_gross_pvs'],  # f_gross_pvs,
                'tot_transactions': dict_rst['n_trs'],  # n_trs,
                'tot_revenue': dict_rst['n_rev'],  # n_rev,
                'tot_registrations': dict_rst['n_reg'],  # n_reg
            }

    def __cleanup_yt_log(self, lst_ga_log):
        lst_ga_log['term'] = '(not set)'  # assume that term from youtube is always '(not set)'
        lst_ga_log['term'] = lst_ga_log['term'].replace(' ', '').upper()
        dict_rst = self.__convert_daily_log(lst_ga_log)
        s_uniq_log_id = dict_rst['s_uniq_log_id']
        if self.__g_dictYtMergedDailyLog.get(s_uniq_log_id, self.__g_sSvNull) != self.__g_sSvNull:  # if sRowId exists
            self.__g_dictYtMergedDailyLog[s_uniq_log_id]['session'] += dict_rst['n_session']  # n_session
            self.__g_dictYtMergedDailyLog[s_uniq_log_id]['tot_new_session'] += dict_rst['f_gross_new']  # f_gross_new
            self.__g_dictYtMergedDailyLog[s_uniq_log_id]['tot_bounce'] += dict_rst['f_gross_b']  # f_gross_b
            self.__g_dictYtMergedDailyLog[s_uniq_log_id]['tot_duration_sec'] += dict_rst['f_gross_dur_sec']  # f_gross_dur_sec
            self.__g_dictYtMergedDailyLog[s_uniq_log_id]['tot_pvs'] += dict_rst['f_gross_pvs']  # f_gross_pvs
            self.__g_dictYtMergedDailyLog[s_uniq_log_id]['tot_transactions'] += dict_rst['n_trs']  # n_trs
            self.__g_dictYtMergedDailyLog[s_uniq_log_id]['tot_revenue'] += dict_rst['n_rev']  # n_rev
            self.__g_dictYtMergedDailyLog[s_uniq_log_id]['tot_registrations'] += dict_rst['n_reg']  # n_reg
        else:  # if new log requested
            self.__g_dictYtMergedDailyLog[s_uniq_log_id] = {
                'session': dict_rst['n_session'],  # n_session,
                'tot_new_session': dict_rst['f_gross_new'],  # f_gross_new,
                'tot_bounce': dict_rst['f_gross_b'],  # f_gross_b,
                'tot_duration_sec': dict_rst['f_gross_dur_sec'],  # f_gross_dur_sec,
                'tot_pvs': dict_rst['f_gross_pvs'],  # f_gross_pvs,
                'tot_transactions': dict_rst['n_trs'],  # n_trs,
                'tot_revenue': dict_rst['n_rev'],  # n_rev,
                'tot_registrations': dict_rst['n_reg'],  # n_reg
            }

    def __cleanup_kko_log(self, lst_ga_log):
        lst_ga_log['term'] = lst_ga_log['term'].replace(' ', '').upper()
        dict_rst = self.__convert_daily_log(lst_ga_log)
        s_uniq_log_id = dict_rst['s_uniq_log_id']
        if self.__g_dictKkoMergedDailyLog.get(s_uniq_log_id, self.__g_sSvNull) != self.__g_sSvNull:  # if sRowId exists
            self.__g_dictKkoMergedDailyLog[s_uniq_log_id]['session'] += dict_rst['n_session']  # n_session
            self.__g_dictKkoMergedDailyLog[s_uniq_log_id]['tot_new_session'] += dict_rst['f_gross_new']  # f_gross_new
            self.__g_dictKkoMergedDailyLog[s_uniq_log_id]['tot_bounce'] += dict_rst['f_gross_b']  # f_gross_b
            self.__g_dictKkoMergedDailyLog[s_uniq_log_id]['tot_duration_sec'] += dict_rst['f_gross_dur_sec']  # f_gross_dur_sec
            self.__g_dictKkoMergedDailyLog[s_uniq_log_id]['tot_pvs'] += dict_rst['f_gross_pvs']  # f_gross_pvs
            self.__g_dictKkoMergedDailyLog[s_uniq_log_id]['tot_transactions'] += dict_rst['n_trs']  # n_trs
            self.__g_dictKkoMergedDailyLog[s_uniq_log_id]['tot_revenue'] += dict_rst['n_rev']  # n_rev
            self.__g_dictKkoMergedDailyLog[s_uniq_log_id]['tot_registrations'] += dict_rst['n_reg']  # n_reg
        else:  # if new log requested
            self.__g_dictKkoMergedDailyLog[s_uniq_log_id] = {
                'session': dict_rst['n_session'],  # n_session,
                'tot_new_session': dict_rst['f_gross_new'],  # f_gross_new,
                'tot_bounce': dict_rst['f_gross_b'],  # f_gross_b,
                'tot_duration_sec': dict_rst['f_gross_dur_sec'],  # f_gross_dur_sec,
                'tot_pvs': dict_rst['f_gross_pvs'],  # f_gross_pvs,
                'tot_transactions': dict_rst['n_trs'],  # n_trs,
                'tot_revenue': dict_rst['n_rev'],  # n_rev,
                'tot_registrations': dict_rst['n_reg'],  # n_reg
            }

    def __cleanup_nvad_log(self, lst_ga_log):
        # lst_ga_log['term'] = lst_ga_log['term'].replace(' ', '').upper()
        s_term = lst_ga_log['term'].replace(' ', '')
        # merge and create new performance row
        if lst_ga_log['media'] == 'cpc' and lst_ga_log['campaign_1st'].find('NVSHOP') > -1:
            lst_nvad_log_daily = self.__g_oSvMysql.execute_query('getNvadLogSpecificNvshop', self.__g_sDate,
                                                                 lst_ga_log['media'], s_term, 'NVSHOP', lst_ga_log['ua'])
            # NVAD does not separate NVSHOPPING item so change term from "nvshop_XXX" to "nvshop"
            if len(lst_nvad_log_daily) == 1:
                # aTerm = s_term.split('_') # it means "nvshop_XXX"
                s_term = 'nvshop'  # it should be 'nvshop' even if a real value is 'nvshopping',
                lst_ga_log['campaign_1st'] = 'NVSHOP'
            del lst_nvad_log_daily
        lst_ga_log['term'] = s_term
        dict_rst = self.__convert_daily_log(lst_ga_log)
        s_uniq_log_id = dict_rst['s_uniq_log_id']
        if self.__g_dictNvadMergedDailyLog.get(s_uniq_log_id, self.__g_sSvNull) != self.__g_sSvNull:  # if sRowId exists
            self.__g_dictNvadMergedDailyLog[s_uniq_log_id]['session'] += dict_rst['n_session']  # n_session
            self.__g_dictNvadMergedDailyLog[s_uniq_log_id]['tot_new_session'] += dict_rst['f_gross_new']  # f_gross_new
            self.__g_dictNvadMergedDailyLog[s_uniq_log_id]['tot_bounce'] += dict_rst['f_gross_b']  # f_gross_b
            self.__g_dictNvadMergedDailyLog[s_uniq_log_id]['tot_duration_sec'] += dict_rst[
                'f_gross_dur_sec']  # f_gross_dur_sec
            self.__g_dictNvadMergedDailyLog[s_uniq_log_id]['tot_pvs'] += dict_rst['f_gross_pvs']  # f_gross_pvs
            self.__g_dictNvadMergedDailyLog[s_uniq_log_id]['tot_transactions'] += dict_rst['n_trs']  # n_trs
            self.__g_dictNvadMergedDailyLog[s_uniq_log_id]['tot_revenue'] += dict_rst['n_rev']  # n_rev
            self.__g_dictNvadMergedDailyLog[s_uniq_log_id]['tot_registrations'] += dict_rst['n_reg']  # n_reg
        else:  # if new log requested
            self.__g_dictNvadMergedDailyLog[s_uniq_log_id] = {
                'session': dict_rst['n_session'],  # n_session,
                'tot_new_session': dict_rst['f_gross_new'],  # f_gross_new,
                'tot_bounce': dict_rst['f_gross_b'],  # f_gross_b,
                'tot_duration_sec': dict_rst['f_gross_dur_sec'],  # f_gross_dur_sec,
                'tot_pvs': dict_rst['f_gross_pvs'],  # f_gross_pvs,
                'tot_transactions': dict_rst['n_trs'],  # n_trs,
                'tot_revenue': dict_rst['n_rev'],  # n_rev,
                'tot_registrations': dict_rst['n_reg'],  # n_reg
            }
        del dict_rst

    def __cleanup_other_log(self, lst_ga_log):
        lst_ga_log['term'] = lst_ga_log['term'].replace(' ', '')
        dict_rst = self.__convert_daily_log(lst_ga_log)
        s_uniq_log_id = dict_rst['s_uniq_log_id']
        if self.__g_dictOtherMergedDailyLog.get(s_uniq_log_id, self.__g_sSvNull) != self.__g_sSvNull:
            self.__g_dictOtherMergedDailyLog[s_uniq_log_id]['session'] += dict_rst['n_session']  # n_session
            self.__g_dictOtherMergedDailyLog[s_uniq_log_id]['tot_new_session'] += dict_rst['f_gross_new']  # f_gross_new
            self.__g_dictOtherMergedDailyLog[s_uniq_log_id]['tot_bounce'] += dict_rst['f_gross_b']  # f_gross_b
            self.__g_dictOtherMergedDailyLog[s_uniq_log_id]['tot_duration_sec'] += dict_rst[
                'f_gross_dur_sec']  # f_gross_dur_sec
            self.__g_dictOtherMergedDailyLog[s_uniq_log_id]['tot_pvs'] += dict_rst['f_gross_pvs']  # f_gross_pvs
            self.__g_dictOtherMergedDailyLog[s_uniq_log_id]['tot_transactions'] += dict_rst['n_trs']  # n_trs
            self.__g_dictOtherMergedDailyLog[s_uniq_log_id]['tot_revenue'] += dict_rst['n_rev']  # n_rev
            self.__g_dictOtherMergedDailyLog[s_uniq_log_id]['tot_registrations'] += dict_rst['n_reg']  # n_reg
        else:  # if new log requested
            self.__g_dictOtherMergedDailyLog[s_uniq_log_id] = {
                'session': dict_rst['n_session'],  # n_session,
                'tot_new_session': dict_rst['f_gross_new'],  # f_gross_new,
                'tot_bounce': dict_rst['f_gross_b'],  # f_gross_b,
                'tot_duration_sec': dict_rst['f_gross_dur_sec'],  # f_gross_dur_sec,
                'tot_pvs': dict_rst['f_gross_pvs'],  # f_gross_pvs,
                'tot_transactions': dict_rst['n_trs'],  # n_trs,
                'tot_revenue': dict_rst['n_rev'],  # n_rev,
                'tot_registrations': dict_rst['n_reg'],  # n_reg
            }
        del dict_rst

    def __merge_fb_log(self):
        dict_fb_daily_log = {}
        lst_fb_log_daily = self.__g_oSvMysql.execute_query('getFbIgLogDaily', self.__g_sDate)
        # for dict_row_tmp in lst_fb_log_daily:
        #     print(dict_row_tmp)
        # print('')
        # print('')
        for dict_log_single in lst_fb_log_daily:
            dict_fb_daily_log[dict_log_single['log_srl']] = {
                'biz_id': dict_log_single['biz_id'], 'ua': dict_log_single['ua'], 'source': dict_log_single['source'],
                'rst_type': dict_log_single['rst_type'], 'media': dict_log_single['media'], 'brd': dict_log_single['brd'],
                'campaign_1st': dict_log_single['campaign_1st'], 'campaign_2nd': dict_log_single['campaign_2nd'],
                'campaign_3rd': dict_log_single['campaign_3rd'],
                'cost': dict_log_single['cost'], 'imp': dict_log_single['imp'], 'click': dict_log_single['click'],
                'conv_cnt': dict_log_single['conv_cnt'], 'conv_amnt': dict_log_single['conv_amnt']
            }
        del lst_fb_log_daily
        # sort FB log dictionary by session number; FB sometimes reports utm_term and GA separate FB campaign code by utm_term
        # dictFbMergedDailyLogSortBySession = sorted(self.__g_dictFbMergedDailyLog.items(), key=(lambda x:x[1]['session']), reverse=True)
        for s_uniq_log_id, dict_row in self.__g_dictFbMergedDailyLog.items():
            dict_id_rst = self.__parse_uniq_log_id(s_uniq_log_id)
            lst_fb_log_daily_source = self.__g_oSvMysql.execute_query('getFbIgLogSpecific', self.__g_sDate,
                                                                      dict_id_rst['s_media'], dict_id_rst['s_source'],
                                                                      dict_id_rst['s_camp_1st'], dict_id_rst['s_camp_2nd'],
                                                                      dict_id_rst['s_camp_3rd'], dict_id_rst['s_ua'])
            n_rec_cnt = len(lst_fb_log_daily_source)
            if n_rec_cnt == 0:
                lst_fb_log_daily_cpc = self.__g_oSvMysql.execute_query('getFbIgLogSpecific', self.__g_sDate, 'cpc',
                                                                       dict_id_rst['s_source'],
                                                                       dict_id_rst['s_camp_1st'],
                                                                       dict_id_rst['s_camp_2nd'],
                                                                       dict_id_rst['s_camp_3rd'],
                                                                       dict_id_rst['s_ua'])
                n_rec_cnt_cpc = len(lst_fb_log_daily_cpc)
                if n_rec_cnt_cpc == 0:
                    self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', dict_id_rst['s_ua'],
                                                    dict_id_rst['s_term'], dict_id_rst['s_source'],
                                                    dict_id_rst['s_rst_type'], 0, '', dict_id_rst['s_media'],
                                                    dict_id_rst['s_brd'], dict_id_rst['s_camp_1st'],
                                                    dict_id_rst['s_camp_2nd'], dict_id_rst['s_camp_3rd'],
                                                    0, 0, 0, 0, 0, 0, 0,
                                                    dict_row['session'], dict_row['tot_new_session'],
                                                    dict_row['tot_bounce'], dict_row['tot_duration_sec'],
                                                    dict_row['tot_pvs'], dict_row['tot_transactions'],
                                                    dict_row['tot_revenue'], dict_row['tot_registrations'],
                                                    self.__g_sDate)
                elif n_rec_cnt_cpc == 1:
                    n_fb_merged_log_srl = lst_fb_log_daily_cpc[0]['log_srl']
                    # Prevent disharmony on FB ad log and GA FB PS log
                    # an ad contents inspection of facebook system sends a test ping 
                    # whenever user modify facebook ad contents without any charging log
                    # but GA identifies this test ping as paid search session
                    # Above disharmony causes a conflict if modify an already-lived ad content
                    if n_fb_merged_log_srl in dict_fb_daily_log:
                        dict_fb_daily_log.pop(n_fb_merged_log_srl)
                        self.__g_oAgencyInfo.load_by_source_id(self.__g_sSvAcctId, self.__g_sBrandId, 'fb_biz',
                                                            lst_fb_log_daily_cpc[0]['biz_id'])
                        dict_cost_rst = self.__g_oAgencyInfo.get_cost_info('fb_biz', lst_fb_log_daily_cpc[0]['biz_id'],
                                                                        self.__g_sDate, lst_fb_log_daily_cpc[0]['cost'])
                        n_media_raw_cost = dict_cost_rst['cost']
                        n_media_agency_fee = dict_cost_rst['agency_fee']
                        n_media_vat = dict_cost_rst['vat']
                    else:
                        n_media_raw_cost = 0
                        n_media_agency_fee = 0
                        n_media_vat = 0
                    self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', dict_id_rst['s_ua'],
                                                    dict_id_rst['s_term'], dict_id_rst['s_source'],
                                                    dict_id_rst['s_rst_type'], dict_cost_rst['agency_id'], dict_cost_rst['agency_name'],
                                                    dict_id_rst['s_media'], dict_id_rst['s_brd'],
                                                    dict_id_rst['s_camp_1st'], dict_id_rst['s_camp_2nd'],
                                                    dict_id_rst['s_camp_3rd'],
                                                    n_media_raw_cost, n_media_agency_fee, n_media_vat,
                                                    lst_fb_log_daily_cpc[0]['imp'], lst_fb_log_daily_cpc[0]['click'],
                                                    lst_fb_log_daily_cpc[0]['conv_cnt'],
                                                    lst_fb_log_daily_cpc[0]['conv_amnt'],
                                                    dict_row['session'], dict_row['tot_new_session'],
                                                    dict_row['tot_bounce'], dict_row['tot_duration_sec'],
                                                    dict_row['tot_pvs'], dict_row['tot_transactions'],
                                                    dict_row['tot_revenue'], dict_row['tot_registrations'],
                                                    self.__g_sDate)
            elif n_rec_cnt == 1:
                n_fb_merged_log_srl = lst_fb_log_daily_source[0]['log_srl']
                if n_fb_merged_log_srl in dict_fb_daily_log:  # if designated log exists
                    dict_fb_daily_log.pop(n_fb_merged_log_srl)
                    self.__g_oAgencyInfo.load_by_source_id(self.__g_sSvAcctId, self.__g_sBrandId, 'fb_biz',
                                                           lst_fb_log_daily_source[0]['biz_id'])
                    dict_cost_rst = self.__g_oAgencyInfo.get_cost_info('fb_biz', lst_fb_log_daily_source[0]['biz_id'],
                                                                       self.__g_sDate,
                                                                       lst_fb_log_daily_source[0]['cost'])
                    n_media_raw_cost = dict_cost_rst['cost']
                    n_media_agency_fee = dict_cost_rst['agency_fee']
                    n_media_vat = dict_cost_rst['vat']
                else:
                    # if designated log has been already popped; FB sometimes reports utm_term and GA separate FB campaign code by utm_term;
                    # self.__g_dictFbMergedDailyLog already has been sorted by session number;
                    # as a result specified cost already has been allocated to the most-sessioned utm_term
                    n_media_raw_cost = 0
                    n_media_agency_fee = 0
                    n_media_vat = 0
                self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', dict_id_rst['s_ua'],
                                                dict_id_rst['s_term'], dict_id_rst['s_source'],
                                                dict_id_rst['s_rst_type'], dict_cost_rst['agency_id'], dict_cost_rst['agency_name'], 
                                                dict_id_rst['s_media'], dict_id_rst['s_brd'],
                                                dict_id_rst['s_camp_1st'], dict_id_rst['s_camp_2nd'],
                                                dict_id_rst['s_camp_3rd'], 
                                                n_media_raw_cost, n_media_agency_fee, n_media_vat,
                                                lst_fb_log_daily_source[0]['imp'],
                                                lst_fb_log_daily_source[0]['click'],
                                                lst_fb_log_daily_source[0]['conv_cnt'],
                                                lst_fb_log_daily_source[0]['conv_amnt'],
                                                dict_row['session'], dict_row['tot_new_session'],
                                                dict_row['tot_bounce'], dict_row['tot_duration_sec'],
                                                dict_row['tot_pvs'], dict_row['tot_transactions'],
                                                dict_row['tot_revenue'], dict_row['tot_registrations'],
                                                self.__g_sDate)
            else:
                self._print_debug('fb record with multiple media data on ' + self.__g_sDate)

        # proc residual - fb api sends log but GA does not detect
        if len(dict_fb_daily_log):
            for s_remaining_log, dict_remaining_row in dict_fb_daily_log.items():
                self.__g_oAgencyInfo.load_by_source_id(self.__g_sSvAcctId, self.__g_sBrandId, 'fb_biz',
                                                       dict_remaining_row['biz_id'])
                dict_cost_rst = self.__g_oAgencyInfo.get_cost_info('fb_biz', dict_remaining_row['biz_id'],
                                                                   self.__g_sDate, dict_remaining_row['cost'])
                s_ua = dict_remaining_row['ua']
                s_source = dict_remaining_row['source']
                s_rst_type = dict_remaining_row['rst_type']
                s_media = dict_remaining_row['media']
                s_brd = dict_remaining_row['brd']
                s_camp_1st = dict_remaining_row['campaign_1st']
                s_camp_2nd = dict_remaining_row['campaign_2nd']
                s_camp_3rd = dict_remaining_row['campaign_3rd']
                s_term = ''
                self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', s_ua, s_term, s_source, s_rst_type,
                                                dict_cost_rst['agency_id'], dict_cost_rst['agency_name'], s_media, s_brd,
                                                s_camp_1st, s_camp_2nd, s_camp_3rd,
                                                dict_cost_rst['cost'], dict_cost_rst['agency_fee'], dict_cost_rst['vat'],
                                                dict_remaining_row['imp'], dict_remaining_row['click'],
                                                dict_remaining_row['conv_cnt'], dict_remaining_row['conv_amnt'],
                                                0, 0, 0, 0, 0, 0, 0, 0, self.__g_sDate)

    def __merge_googleads_log(self):
        # assume that google-ads log is identified by term and UA basically
        # GDN campaign sometimes is identified by Campaign Code and UA
        # this is very different with YOUTUBE campaign from google-ads
        dict_gads_log_daily = {}
        lst_googleads_log_daily = self.__g_oSvMysql.execute_query('getAdwLogDaily',
                                                                  self.__g_sDate, self.__g_dictSourceTitleTag['google'])
        for dictSingleLog in lst_googleads_log_daily:
            dict_gads_log_daily[dictSingleLog['log_srl']] = {
                'customer_id': dictSingleLog['customer_id'], 'ua': dictSingleLog['ua'], 'term': dictSingleLog['term'],
                'rst_type': dictSingleLog['rst_type'], 'media': dictSingleLog['media'], 'brd': dictSingleLog['brd'],
                'campaign_1st': dictSingleLog['campaign_1st'], 'campaign_2nd': dictSingleLog['campaign_2nd'],
                'campaign_3rd': dictSingleLog['campaign_3rd'], 'cost': dictSingleLog['cost'],
                'imp': dictSingleLog['imp'],
                'click': dictSingleLog['click'], 'conv_cnt': dictSingleLog['conv_cnt'],
                'conv_amnt': dictSingleLog['conv_amnt']
            }
        for s_uniq_log_id, dict_row in self.__g_dictAdwMergedDailyLog.items():
            dict_id_rst = self.__parse_uniq_log_id(s_uniq_log_id)
            if dict_id_rst['s_camp_1st'].find('GDN') > -1:  # if the campaign is related with GDN
                lst_gads_log_daily_temp = self.__g_oSvMysql.execute_query('getAdwLogSpecificRmk', self.__g_sDate,
                                                                          self.__g_dictSourceTitleTag['google'],
                                                                          dict_id_rst['s_media'],
                                                                          dict_id_rst['s_camp_1st'],
                                                                          dict_id_rst['s_camp_2nd'],
                                                                          dict_id_rst['s_camp_3rd'], dict_id_rst['s_ua'])
                n_rec_cnt = len(lst_gads_log_daily_temp)
                if n_rec_cnt == 0:
                    lst_googleads_log_daily = []
                elif n_rec_cnt == 1:
                    lst_googleads_log_daily = lst_gads_log_daily_temp  # replace log
                elif n_rec_cnt > 1:
                    lst_googleads_log_daily = []
                    n_gross_cost = 0
                    n_gross_imp = 0
                    n_gross_click = 0
                    n_gross_conv_cnt = 0
                    n_gross_conv_amnt = 0
                    for lst_gads_log_single in lst_gads_log_daily_temp:
                        n_gross_cost += lst_gads_log_single['cost']
                        n_gross_imp += lst_gads_log_single['imp']
                        n_gross_click += lst_gads_log_single['click']
                        n_gross_conv_cnt += lst_gads_log_single['conv_cnt']
                        n_gross_conv_amnt += lst_gads_log_single['conv_amnt']
                    lst_googleads_log_daily.append({
                        'customer_id': lst_gads_log_single['customer_id'], 'cost': n_gross_cost, 'imp': n_gross_imp,
                        'click': n_gross_click, 'conv_cnt': n_gross_conv_cnt, 'conv_amnt': n_gross_conv_amnt
                    })
            else:  # if the campaign is related with normal google-ads
                lst_googleads_log_daily = self.__g_oSvMysql.execute_query('getAdwLogSpecific', self.__g_sDate,
                                                                          self.__g_dictSourceTitleTag['google'],
                                                                          dict_id_rst['s_media'], dict_id_rst['s_term'],
                                                                          dict_id_rst['s_camp_1st'],
                                                                          dict_id_rst['s_camp_2nd'],
                                                                          dict_id_rst['s_camp_3rd'], dict_id_rst['s_ua'])
            n_rec_cnt = len(lst_googleads_log_daily)
            if n_rec_cnt == 0:  # add new non-media bounded log, if unable to find related ADW raw log
                self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', dict_id_rst['s_ua'],
                                                dict_id_rst['s_term'], 'google', dict_id_rst['s_rst_type'], 0, '',
                                                dict_id_rst['s_media'], dict_id_rst['s_brd'], dict_id_rst['s_camp_1st'],
                                                dict_id_rst['s_camp_2nd'], dict_id_rst['s_camp_3rd'],
                                                0, 0, 0, 0, 0, 0, 0,
                                                dict_row['session'], dict_row['tot_new_session'], dict_row['tot_bounce'],
                                                dict_row['tot_duration_sec'], dict_row['tot_pvs'],
                                                dict_row['tot_transactions'], dict_row['tot_revenue'],
                                                dict_row['tot_registrations'], self.__g_sDate)
            elif n_rec_cnt == 1:  # add new media bounded log, if able to find related ADW raw log
                if 'log_srl' in lst_googleads_log_daily[0]:
                    dict_gads_log_daily.pop(lst_googleads_log_daily[0]['log_srl'])
                else:
                    for lstAdwLogSingle in lst_gads_log_daily_temp:
                        dict_gads_log_daily.pop(lstAdwLogSingle['log_srl'])
                self.__g_oAgencyInfo.load_by_source_id(self.__g_sSvAcctId, self.__g_sBrandId, 'adwords',
                                                       lst_googleads_log_daily[0]['customer_id'])
                dict_cost_rst = self.__g_oAgencyInfo.get_cost_info('adwords', lst_googleads_log_daily[0]['customer_id'],
                                                                   self.__g_sDate, lst_googleads_log_daily[0]['cost'])
                self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', dict_id_rst['s_ua'],
                                                dict_id_rst['s_term'], 'google', dict_id_rst['s_rst_type'],
                                                dict_cost_rst['agency_id'], dict_cost_rst['agency_name'],
                                                dict_id_rst['s_media'], dict_id_rst['s_brd'],
                                                dict_id_rst['s_camp_1st'], dict_id_rst['s_camp_2nd'], dict_id_rst['s_camp_3rd'],
                                                dict_cost_rst['cost'], dict_cost_rst['agency_fee'], dict_cost_rst['vat'],
                                                lst_googleads_log_daily[0]['imp'], lst_googleads_log_daily[0]['click'],
                                                lst_googleads_log_daily[0]['conv_cnt'],
                                                lst_googleads_log_daily[0]['conv_amnt'],
                                                dict_row['session'], dict_row['tot_new_session'], dict_row['tot_bounce'],
                                                dict_row['tot_duration_sec'], dict_row['tot_pvs'],
                                                dict_row['tot_transactions'], dict_row['tot_revenue'],
                                                dict_row['tot_registrations'], self.__g_sDate)
            else:
                self._print_debug('adw record with multiple media data on ' + self.__g_sDate)
                self._print_debug(dict_id_rst)
                self._print_debug(dict_row)
                self._print_debug(lst_googleads_log_daily)
        # proc residual
        # means mob_brded':0, 'mob_nonbrded':0, 'pc_brded':0, 'pc_nonbrded
        dict_imp = {'M_1': 0, 'M_0': 0, 'P_1': 0, 'P_0': 0}
        for n_log_srl in dict_gads_log_daily:
            if dict_gads_log_daily[n_log_srl]['cost'] > 0:
                self.__g_oAgencyInfo.load_by_source_id(self.__g_sSvAcctId, self.__g_sBrandId, 'adwords',
                                                       dict_gads_log_daily[n_log_srl]['customer_id'])
                dict_cost_rst = self.__g_oAgencyInfo.get_cost_info('adwords',
                                                                   dict_gads_log_daily[n_log_srl]['customer_id'],
                                                                   self.__g_sDate,
                                                                   dict_gads_log_daily[n_log_srl]['cost'])
                self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', dict_gads_log_daily[n_log_srl]['ua'],
                                                dict_gads_log_daily[n_log_srl]['term'], 'google',
                                                dict_gads_log_daily[n_log_srl]['rst_type'],
                                                dict_cost_rst['agency_id'], dict_cost_rst['agency_name'],
                                                dict_gads_log_daily[n_log_srl]['media'],
                                                dict_gads_log_daily[n_log_srl]['brd'],
                                                dict_gads_log_daily[n_log_srl]['campaign_1st'],
                                                dict_gads_log_daily[n_log_srl]['campaign_2nd'],
                                                dict_gads_log_daily[n_log_srl]['campaign_3rd'],
                                                dict_cost_rst['cost'], dict_cost_rst['agency_fee'], dict_cost_rst['vat'],
                                                dict_gads_log_daily[n_log_srl]['imp'],
                                                dict_gads_log_daily[n_log_srl]['click'],
                                                dict_gads_log_daily[n_log_srl]['conv_cnt'],
                                                dict_gads_log_daily[n_log_srl]['conv_amnt'],
                                                0, 0, 0, 0, 0, 0, 0, 0, self.__g_sDate)
            elif dict_gads_log_daily[n_log_srl]['cost'] == 0:
                s_indication = dict_gads_log_daily[n_log_srl]['ua'] + '_' + str(dict_gads_log_daily[n_log_srl]['brd'])
                dict_imp[s_indication] = dict_imp[s_indication] + int(dict_gads_log_daily[n_log_srl]['imp'])

        for s_idx in dict_imp:
            if dict_imp[s_idx] > 0:
                a_idx = s_idx.split('_')
                s_ua = a_idx[0]
                b_brded = a_idx[1]
                self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', s_ua, '|@|sv', 'google', 'PS', 0, '',
                                                'cpc', b_brded, '|@|sv', '|@|sv', '|@|sv', 0, 0, 0, dict_imp[s_idx],
                                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, self.__g_sDate)

    def __merge_yt_log(self):
        # assume that YOUTUBE log is identified by Campaign Code and UA always
        # it is very different with google-ads campaign
        dict_yt_daily_log = {}
        # assume that term from youtube is always '(not set)'
        lst_yt_log_daily = self.__g_oSvMysql.execute_query('getAdwLogDaily',
                                                           self.__g_sDate, self.__g_dictSourceTitleTag['youtube'])
        for dictSingleLog in lst_yt_log_daily:
            dict_yt_daily_log[dictSingleLog['log_srl']] = {
                'customer_id': dictSingleLog['customer_id'], 'ua': dictSingleLog['ua'],
                'rst_type': dictSingleLog['rst_type'],  # 'term':dictSingleLog['term'],
                'media': dictSingleLog['media'], 'brd': dictSingleLog['brd'],
                'campaign_1st': dictSingleLog['campaign_1st'],
                'campaign_2nd': dictSingleLog['campaign_2nd'], 'campaign_3rd': dictSingleLog['campaign_3rd'],
                'cost': dictSingleLog['cost'], 'imp': dictSingleLog['imp'], 'click': dictSingleLog['click'],
                'conv_cnt': dictSingleLog['conv_cnt'], 'conv_amnt': dictSingleLog['conv_amnt']
            }

        for s_uniq_log_id, dict_row in self.__g_dictYtMergedDailyLog.items():
            dict_id_rst = self.__parse_uniq_log_id(s_uniq_log_id)
            s_term = '(not set)'  # assume that term from youtube is always '(not set)'
            lst_yt_log_daily_tep = self.__g_oSvMysql.execute_query('getAdwLogSpecificRmk',
                                                                   self.__g_sDate, self.__g_dictSourceTitleTag['youtube'],
                                                                   dict_id_rst['s_media'], dict_id_rst['s_camp_1st'],
                                                                   dict_id_rst['s_camp_2nd'], dict_id_rst['s_camp_3rd'],
                                                                   dict_id_rst['s_ua'])
            n_rec_cnt = len(lst_yt_log_daily_tep)
            if n_rec_cnt == 0:
                lst_yt_log_daily = []
            elif n_rec_cnt == 1:
                lst_yt_log_daily = lst_yt_log_daily_tep  # replace log
            elif n_rec_cnt > 1:
                lst_yt_log_daily = []
                n_gross_cost = 0
                n_gross_imp = 0
                n_gross_click = 0
                n_gross_conv_cnt = 0
                n_gross_conv_amnt = 0
                for lst_yt_log_single in lst_yt_log_daily_tep:
                    n_gross_cost += lst_yt_log_single['cost']
                    n_gross_imp += lst_yt_log_single['imp']
                    n_gross_click += lst_yt_log_single['click']
                    n_gross_conv_cnt += lst_yt_log_single['conv_cnt']
                    n_gross_conv_amnt += lst_yt_log_single['conv_amnt']
                lst_yt_log_daily.append({
                    'customer_id': lst_yt_log_single['customer_id'], 'cost': n_gross_cost, 'imp': n_gross_imp,
                    'click': n_gross_click, 'conv_cnt': n_gross_conv_cnt, 'conv_amnt': n_gross_conv_amnt
                })
            n_rec_cnt = len(lst_yt_log_daily)
            if n_rec_cnt == 0:
                self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', dict_id_rst['s_ua'], s_term, 'youtube',
                                                dict_id_rst['s_rst_type'], 0, '', dict_id_rst['s_media'],
                                                dict_id_rst['s_brd'], dict_id_rst['s_camp_1st'],
                                                dict_id_rst['s_camp_2nd'], dict_id_rst['s_camp_3rd'],
                                                0, 0, 0, 0, 0, 0, 0,
                                                dict_row['session'], dict_row['tot_new_session'], dict_row['tot_bounce'],
                                                dict_row['tot_duration_sec'], dict_row['tot_pvs'],
                                                dict_row['tot_transactions'], dict_row['tot_revenue'],
                                                dict_row['tot_registrations'], self.__g_sDate)
            elif n_rec_cnt == 1:
                # if lst_yt_log_daily[0]['log_srl'] in dict_yt_daily_log:
                if 'log_srl' in lst_yt_log_daily[0]:
                    dict_yt_daily_log.pop(lst_yt_log_daily[0]['log_srl'])
                else:
                    for lstYtLogSingle in lst_yt_log_daily_tep:
                        if lstYtLogSingle['log_srl'] in dict_yt_daily_log:
                            dict_yt_daily_log.pop(lstYtLogSingle['log_srl'])
                        else:
                            # 비슷한 캠페인 명칭에 이미 비용이 할당되었기 때문에 0비용으로 처리함
                            # ex) YT_PS_DISP_JOX[빈칸]_INDOOR_DISINFECT  vs YT_PS_DISP_JOX_INDOOR_DISINFECT
                            lst_yt_log_daily[0]['cost'] = 0
                self.__g_oAgencyInfo.load_by_source_id(self.__g_sSvAcctId, self.__g_sBrandId, 'adwords',
                                                       lst_yt_log_daily[0]['customer_id'])
                dict_cost_rst = self.__g_oAgencyInfo.get_cost_info('adwords', lst_yt_log_daily[0]['customer_id'],
                                                                   self.__g_sDate, lst_yt_log_daily[0]['cost'])
                self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', dict_id_rst['s_ua'], s_term, 'youtube',
                                                dict_id_rst['s_rst_type'], dict_cost_rst['agency_id'], dict_cost_rst['agency_name'],
                                                dict_id_rst['s_media'], dict_id_rst['s_brd'],
                                                dict_id_rst['s_camp_1st'], dict_id_rst['s_camp_2nd'],
                                                dict_id_rst['s_camp_3rd'],
                                                dict_cost_rst['cost'], dict_cost_rst['agency_fee'], dict_cost_rst['vat'],
                                                lst_yt_log_daily[0]['imp'], lst_yt_log_daily[0]['click'],
                                                lst_yt_log_daily[0]['conv_cnt'], lst_yt_log_daily[0]['conv_amnt'],
                                                dict_row['session'], dict_row['tot_new_session'], dict_row['tot_bounce'],
                                                dict_row['tot_duration_sec'], dict_row['tot_pvs'],
                                                dict_row['tot_transactions'],
                                                dict_row['tot_revenue'], dict_row['tot_registrations'], self.__g_sDate)
            else:
                self._print_debug('youtube record with multiple media data on ' + self.__g_sDate)
                self._print_debug(dict_id_rst)
                self._print_debug(dict_row)
                self._print_debug(lst_yt_log_daily)
        # proc residual
        dict_yt_residual_arranged = {}
        for n_log_srl, dict_single_yt_log in dict_yt_daily_log.items():
            s_uniq_log_id = dict_single_yt_log['customer_id'] + SEPARATOR + dict_single_yt_log['ua'] + SEPARATOR + \
                     dict_single_yt_log['media'] + SEPARATOR + dict_single_yt_log['rst_type'] + SEPARATOR + \
                     str(dict_single_yt_log['brd']) + SEPARATOR + dict_single_yt_log['campaign_1st'] + SEPARATOR + \
                     dict_single_yt_log['campaign_2nd'] + SEPARATOR + dict_single_yt_log['campaign_3rd']
            n_cost = int(dict_single_yt_log['cost'])
            n_imp = int(dict_single_yt_log['imp'])
            n_click = int(dict_single_yt_log['click'])
            n_conv_cnt = int(dict_single_yt_log['conv_cnt'])
            n_conv_amnt = int(dict_single_yt_log['conv_amnt'])
            if dict_yt_residual_arranged.get(s_uniq_log_id, self.__g_sSvNull) != self.__g_sSvNull:  # if sRowId exists
                dict_yt_residual_arranged[s_uniq_log_id]['cost'] += n_cost
                dict_yt_residual_arranged[s_uniq_log_id]['imp'] += n_imp
                dict_yt_residual_arranged[s_uniq_log_id]['click'] += n_click
                dict_yt_residual_arranged[s_uniq_log_id]['conv_cnt'] += n_conv_cnt
                dict_yt_residual_arranged[s_uniq_log_id]['conv_amnt'] += n_conv_amnt
            else:  # if new log requested
                dict_yt_residual_arranged[s_uniq_log_id] = {
                    'cost': n_cost, 'imp': n_imp, 'click': n_click, 'conv_cnt': n_conv_cnt, 'conv_amnt': n_conv_amnt
                }
        for s_residual_yt_log_id, dict_single_arranged_log in dict_yt_residual_arranged.items():
            a_row_id = s_residual_yt_log_id.split(SEPARATOR)
            s_cid = a_row_id[0]
            s_ua = a_row_id[1]
            s_media = a_row_id[2]
            s_rst_type = a_row_id[3]
            s_brd = a_row_id[4]
            s_camp_1st = a_row_id[5]
            s_camp_2nd = a_row_id[6]
            s_camp_3rd = a_row_id[7]
            s_term = '(not set)'  # assume that term from youtube is always '(not set)'
            n_agency_id = 0
            s_agency_name = ''
            f_cost = 0.0
            f_agency_fee = 0.0
            f_vat = 0.0
            if dict_single_arranged_log['cost'] > 0:  # but assume that youtube does not provide free impression
                self.__g_oAgencyInfo.load_by_source_id(self.__g_sSvAcctId, self.__g_sBrandId, 'adwords', s_cid)
                dict_cost_rst = self.__g_oAgencyInfo.get_cost_info('adwords', s_cid, self.__g_sDate,
                                                                   dict_single_arranged_log['cost'])
                n_agency_id = dict_cost_rst['agency_id']
                s_agency_name = dict_cost_rst['agency_name']
                f_cost = dict_cost_rst['cost']
                f_agency_fee = dict_cost_rst['agency_fee']
                f_vat = dict_cost_rst['vat']
            self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', s_ua, s_term, 'youtube', s_rst_type,
                                            n_agency_id, s_agency_name, s_media, s_brd, s_camp_1st, s_camp_2nd,
                                            s_camp_3rd, f_cost, f_agency_fee, f_vat,
                                            dict_single_arranged_log['imp'], dict_single_arranged_log['click'],
                                            dict_single_arranged_log['conv_cnt'], dict_single_arranged_log['conv_amnt'],
                                            0, 0, 0, 0, 0, 0, 0, 0, self.__g_sDate)

    def __merge_kko_log(self):
        dict_kko_daily_log = {}
        # kakao itself does not differ by UA
        lst_kko_log_daily = self.__g_oSvMysql.execute_query('getKkoLogDaily', self.__g_sDate)
        for dict_log_single in lst_kko_log_daily:
            if len(dict_log_single['term']) == 0:
                dict_log_single['term'] = '(not set)'
            s_uniq_log_id = dict_log_single['ua'] + SEPARATOR + 'kakao' + SEPARATOR + dict_log_single['rst_type'] + \
                            SEPARATOR + dict_log_single['media'] + SEPARATOR + str(dict_log_single['brd']) + \
                            SEPARATOR + dict_log_single['campaign_1st'] + SEPARATOR + \
                            dict_log_single['campaign_2nd'] + SEPARATOR + dict_log_single['campaign_3rd'] + \
                            SEPARATOR + dict_log_single['term']
            dict_kko_daily_log[s_uniq_log_id] = {
                'customer_id': dict_log_single['customer_id'], 'cost_inc_vat': dict_log_single['cost_inc_vat'],
                'imp': dict_log_single['imp'], 'click': dict_log_single['click'],
                'conv_cnt_direct': dict_log_single['conv_cnt_direct'],
                'conv_amnt_direct': dict_log_single['conv_amnt_direct']
            }
            # dict_log_single['source'] = 'kakao'
            # dict_rst = self.__convert_daily_log(dict_log_single)
            # s_uniq_log_id = dict_rst['s_uniq_log_id']
            # del dict_rst
            # dict_kko_daily_log[s_uniq_log_id] = {
            #     'customer_id': dict_log_single['customer_id'], 'cost_inc_vat': dict_log_single['cost_inc_vat'],
            #     'imp': dict_log_single['imp'], 'click': dict_log_single['click'],
            #     'conv_cnt_direct': dict_log_single['conv_cnt_direct'],
            #     'conv_amnt_direct': dict_log_single['conv_amnt_direct']
            # }
        for s_uniq_log_id, dict_single_row in self.__g_dictKkoMergedDailyLog.items():
            dict_id_rst = self.__parse_uniq_log_id(s_uniq_log_id)
            n_rec_cnt = len(lst_kko_log_daily)
            if n_rec_cnt == 0:  # GA log exists without KKO PS data
                self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', dict_id_rst['s_ua'],
                                                dict_id_rst['s_term'], 'kakao', dict_id_rst['s_rst_type'], 0, '',
                                                dict_id_rst['s_media'], dict_id_rst['s_brd'], dict_id_rst['s_camp_1st'],
                                                dict_id_rst['s_camp_2nd'], dict_id_rst['s_camp_3rd'],
                                                0, 0, 0, 0, 0, 0, 0,
                                                dict_single_row['session'], dict_single_row['tot_new_session'],
                                                dict_single_row['tot_bounce'], dict_single_row['tot_duration_sec'],
                                                dict_single_row['tot_pvs'], dict_single_row['tot_transactions'],
                                                dict_single_row['tot_revenue'], dict_single_row['tot_registrations'],
                                                self.__g_sDate)
            else:  # GA log exists with KKO PS data
                if s_uniq_log_id in dict_kko_daily_log:  # if designated log already created
                    self.__g_oAgencyInfo.load_by_source_id(self.__g_sSvAcctId, self.__g_sBrandId, 'kakao',
                                                           dict_kko_daily_log[s_uniq_log_id]['customer_id'])
                    dict_cost_rst = self.__g_oAgencyInfo.get_cost_info('kakao',
                                                                       dict_kko_daily_log[s_uniq_log_id]['customer_id'],
                                                                       self.__g_sDate,
                                                                       dict_kko_daily_log[s_uniq_log_id]['cost_inc_vat'])
                    self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', dict_id_rst['s_ua'],
                                                    dict_id_rst['s_term'], 'kakao', dict_id_rst['s_rst_type'],
                                                    dict_cost_rst['agency_id'], dict_cost_rst['agency_name'], dict_id_rst['s_media'],
                                                    dict_id_rst['s_brd'], dict_id_rst['s_camp_1st'],
                                                    dict_id_rst['s_camp_2nd'], dict_id_rst['s_camp_3rd'],
                                                    dict_cost_rst['cost'], dict_cost_rst['agency_fee'], dict_cost_rst['vat'],
                                                    dict_kko_daily_log[s_uniq_log_id]['imp'],
                                                    dict_kko_daily_log[s_uniq_log_id]['click'],
                                                    dict_kko_daily_log[s_uniq_log_id]['conv_cnt_direct'],
                                                    dict_kko_daily_log[s_uniq_log_id]['conv_amnt_direct'],
                                                    dict_single_row['session'], dict_single_row['tot_new_session'],
                                                    dict_single_row['tot_bounce'], dict_single_row['tot_duration_sec'],
                                                    dict_single_row['tot_pvs'], dict_single_row['tot_transactions'],
                                                    dict_single_row['tot_revenue'], dict_single_row['tot_registrations'],
                                                    self.__g_sDate)
                    dict_kko_daily_log.pop(s_uniq_log_id)  # removed registered log
                else:  # if new log requested
                    self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', dict_id_rst['s_ua'],
                                                    dict_id_rst['s_term'], 'kakao', dict_id_rst['s_rst_type'], 0, '',
                                                    dict_id_rst['s_media'], dict_id_rst['s_brd'],
                                                    dict_id_rst['s_camp_1st'], dict_id_rst['s_camp_2nd'],
                                                    dict_id_rst['s_camp_3rd'], 0, 0, 0, 0, 0, 0, 0,
                                                    dict_single_row['session'], dict_single_row['tot_new_session'],
                                                    dict_single_row['tot_bounce'], dict_single_row['tot_duration_sec'],
                                                    dict_single_row['tot_pvs'], dict_single_row['tot_transactions'],
                                                    dict_single_row['tot_revenue'], dict_single_row['tot_registrations'],
                                                    self.__g_sDate)
        # proc residual minor kakao moment log
        # means mob_brded':0, 'mob_nonbrded':0, 'pc_brded':0, 'pc_nonbrded
        dict_imp = {'M_1': 0, 'M_0': 0, 'P_1': 0, 'P_0': 0}
        for s_uniq_log_id in dict_kko_daily_log:  # register residual; means KKO MOMENT data without GA log
            dict_id_rst = self.__parse_uniq_log_id(s_uniq_log_id)
            if dict_kko_daily_log[s_uniq_log_id]['cost_inc_vat'] > 0:
                self.__g_oAgencyInfo.load_by_source_id(self.__g_sSvAcctId, self.__g_sBrandId, 'kakao',
                                                       dict_kko_daily_log[s_uniq_log_id]['customer_id'])
                dict_cost_rst = self.__g_oAgencyInfo.get_cost_info('kakao',
                                                                   dict_kko_daily_log[s_uniq_log_id]['customer_id'],
                                                                   self.__g_sDate,
                                                                   dict_kko_daily_log[s_uniq_log_id]['cost_inc_vat'])
                self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', dict_id_rst['s_ua'],
                                                dict_id_rst['s_term'], dict_id_rst['s_source'],
                                                dict_id_rst['s_rst_type'], dict_cost_rst['agency_id'], dict_cost_rst['agency_name'],
                                                dict_id_rst['s_media'], dict_id_rst['s_brd'], dict_id_rst['s_camp_1st'],
                                                dict_id_rst['s_camp_2nd'], dict_id_rst['s_camp_3rd'],
                                                dict_cost_rst['cost'], dict_cost_rst['agency_fee'], dict_cost_rst['vat'],
                                                dict_kko_daily_log[s_uniq_log_id]['imp'],
                                                dict_kko_daily_log[s_uniq_log_id]['click'],
                                                dict_kko_daily_log[s_uniq_log_id]['conv_cnt_direct'],
                                                dict_kko_daily_log[s_uniq_log_id]['conv_amnt_direct'],
                                                0, 0, 0, 0, 0, 0, 0, 0,
                                                self.__g_sDate)
            elif dict_kko_daily_log[s_uniq_log_id]['cost_inc_vat'] == 0:
                s_indication = dict_id_rst['s_ua'] + '_' + dict_id_rst['s_brd']
                dict_imp[s_indication] = dict_imp[s_indication] + int(dict_kko_daily_log[s_uniq_log_id]['imp'])
        try:
            for s_idx in dict_imp:
                if dict_imp[s_idx] > 0:
                    a_idx = s_idx.split('_')
                    s_ua = a_idx[0]
                    b_brded = a_idx[1]
                    self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', s_ua, '|@|sv', 'kakao', 'PS', 0, '',
                                                    'cpc', b_brded, '|@|sv', '|@|sv', '|@|sv', 0, 0, 0, dict_imp[s_idx],
                                                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, self.__g_sDate)
        except NameError:  # if imp only log not exists
            pass

    def __merge_nvad_log(self):
        dict_nvad_daily_log = {}
        lst_nvad_log_daily = self.__g_oSvMysql.execute_query('getNvadLogDaily', self.__g_sDate)
        for dict_single_log in lst_nvad_log_daily:
            dict_nvad_daily_log[dict_single_log['log_srl']] = {
                'customer_id': dict_single_log['customer_id'], 'ua': dict_single_log['ua'],
                'term': dict_single_log['term'], 'rst_type': dict_single_log['rst_type'],
                'media': dict_single_log['media'], 'brd': dict_single_log['brd'],
                'campaign_1st': dict_single_log['campaign_1st'], 'campaign_2nd': dict_single_log['campaign_2nd'],
                'campaign_3rd': dict_single_log['campaign_3rd'], 'cost': dict_single_log['cost'],
                'imp': dict_single_log['imp'], 'click': dict_single_log['click'],
                'conv_cnt': dict_single_log['conv_cnt'], 'conv_amnt': dict_single_log['conv_amnt']
            }
        for s_unique_tag, dict_row in self.__g_dictNvadMergedDailyLog.items():
            dict_id_rst = self.__parse_uniq_log_id(s_unique_tag)
            if dict_id_rst['s_media'] == 'display' and dict_id_rst['s_camp_1st'] == 'BRS':
                lst_nvad_log_daily = self.__g_oSvMysql.execute_query('getNvadLogSpecificDisplay', self.__g_sDate,
                                                                     dict_id_rst['s_media'], dict_id_rst['s_term'],
                                                                     dict_id_rst['s_camp_1st'],
                                                                     dict_id_rst['s_camp_2nd'], dict_id_rst['s_ua'])
            # merge and create new performance row
            elif dict_id_rst['s_media'] == 'cpc' and dict_id_rst['s_camp_1st'] == 'NVSHOP' or \
                    dict_id_rst['s_camp_1st'] == 'NVSHOPPING':
                s_camp_1st = 'NVSHOP'  # unify old and confusing old NVSHOP naming convention
                lst_nvad_log_daily = self.__g_oSvMysql.execute_query('getNvadLogSpecificNvshop', self.__g_sDate,
                                                                     dict_id_rst['s_media'], dict_id_rst['s_term'],
                                                                     dict_id_rst['s_camp_1st'], dict_id_rst['s_ua'])
                if len(lst_nvad_log_daily) > 1:
                    pass  # self._print_debug('NVAD separates NVSHOPPING item so allocate appropriately')
            else:
                lst_nvad_log_daily = self.__g_oSvMysql.execute_query('getNvadLogSpecificCpc', self.__g_sDate,
                                                                     dict_id_rst['s_media'], dict_id_rst['s_term'],
                                                                     dict_id_rst['s_camp_1st'], dict_id_rst['s_camp_2nd'],
                                                                     dict_id_rst['s_camp_3rd'], dict_id_rst['s_ua'])

            n_rec_cnt = len(lst_nvad_log_daily)
            if n_rec_cnt == 0:
                self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', dict_id_rst['s_ua'],
                                                dict_id_rst['s_term'], 'naver', dict_id_rst['s_rst_type'], 0, '',
                                                dict_id_rst['s_media'], dict_id_rst['s_brd'], dict_id_rst['s_camp_1st'],
                                                dict_id_rst['s_camp_2nd'], dict_id_rst['s_camp_3rd'],
                                                0, 0, 0, 0, 0, 0, 0,
                                                dict_row['session'], dict_row['tot_new_session'], dict_row['tot_bounce'],
                                                dict_row['tot_duration_sec'], dict_row['tot_pvs'],
                                                dict_row['tot_transactions'], dict_row['tot_revenue'],
                                                dict_row['tot_registrations'], self.__g_sDate)
            elif n_rec_cnt == 1:
                if lst_nvad_log_daily[0]['log_srl'] in dict_nvad_daily_log:
                    dict_nvad_daily_log.pop(lst_nvad_log_daily[0]['log_srl'])
                else:  # except KeyError:
                    # nvad uppers all alphabet but GA does not, that's why sometime GA log cant match proper NVAD log,
                    # it this case, bot will look up into remaining log by term
                    s_uppered_term_from_ga = dict_id_rst['s_term'].upper()
                    for nLogSrl in dict_nvad_daily_log:
                        if s_uppered_term_from_ga == dict_nvad_daily_log[nLogSrl]['term']:
                            dict_nvad_daily_log.pop(nLogSrl)
                            break
                # finally:
                self.__g_oAgencyInfo.load_by_source_id(self.__g_sSvAcctId, self.__g_sBrandId,
                                                       'naver_ad', lst_nvad_log_daily[0]['customer_id'])
                dict_cost_rst = self.__g_oAgencyInfo.get_cost_info('naver_ad',
                                                                   lst_nvad_log_daily[0]['customer_id'],
                                                                   self.__g_sDate, lst_nvad_log_daily[0]['cost'])
                self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', dict_id_rst['s_ua'],
                                                dict_id_rst['s_term'], 'naver', dict_id_rst['s_rst_type'],
                                                dict_cost_rst['agency_id'], dict_cost_rst['agency_name'], dict_id_rst['s_media'], dict_id_rst['s_brd'],
                                                dict_id_rst['s_camp_1st'], dict_id_rst['s_camp_2nd'],
                                                dict_id_rst['s_camp_3rd'], dict_cost_rst['cost'],
                                                dict_cost_rst['agency_fee'], dict_cost_rst['vat'],
                                                lst_nvad_log_daily[0]['imp'], lst_nvad_log_daily[0]['click'],
                                                lst_nvad_log_daily[0]['conv_cnt'], lst_nvad_log_daily[0]['conv_amnt'],
                                                dict_row['session'], dict_row['tot_new_session'], dict_row['tot_bounce'],
                                                dict_row['tot_duration_sec'], dict_row['tot_pvs'],
                                                dict_row['tot_transactions'],
                                                dict_row['tot_revenue'], dict_row['tot_registrations'], self.__g_sDate)
            else:
                # if 3rd campaign code of the NVR BRS ad group name is changed in busy hour,
                # there would be two log with same term and different 3rd campaign code
                if dict_id_rst['s_media'] == 'display' and dict_id_rst['s_camp_1st'] == 'BRS':
                    dict_brs_log = {'cost': 0, 'imp': 0, 'click': 0, 'conv_cnt': 0, 'conv_amnt': 0}
                    for dict_dupli_nv_brs_log in lst_nvad_log_daily:
                        dict_brs_log['cost'] = dict_brs_log['cost'] + dict_dupli_nv_brs_log['cost']
                        dict_brs_log['imp'] = dict_brs_log['imp'] + dict_dupli_nv_brs_log['imp']
                        dict_brs_log['click'] = dict_brs_log['click'] + dict_dupli_nv_brs_log['click']
                        dict_brs_log['conv_cnt'] = dict_brs_log['conv_cnt'] + dict_dupli_nv_brs_log['conv_cnt']
                        dict_brs_log['conv_amnt'] = dict_brs_log['conv_amnt'] + dict_dupli_nv_brs_log['conv_amnt']
                    self.__g_oAgencyInfo.load_by_source_id(self.__g_sSvAcctId, self.__g_sBrandId,
                                                           'naver_ad', lst_nvad_log_daily[0]['customer_id'])
                    dict_cost_rst = self.__g_oAgencyInfo.get_cost_info('naver_ad',
                                                                       lst_nvad_log_daily[0]['customer_id'],
                                                                       self.__g_sDate, dict_brs_log['cost'])
                    self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', dict_id_rst['s_ua'],
                                                    dict_id_rst['s_term'], 'naver', dict_id_rst['s_rst_type'],
                                                    dict_cost_rst['agency_id'], dict_cost_rst['agency_name'], dict_id_rst['s_media'],
                                                    dict_id_rst['s_brd'], dict_id_rst['s_camp_1st'],
                                                    dict_id_rst['s_camp_2nd'], dict_id_rst['s_camp_3rd'],
                                                    dict_cost_rst['cost'], dict_cost_rst['agency_fee'], dict_cost_rst['vat'],
                                                    dict_brs_log['imp'],  dict_brs_log['click'],
                                                    dict_brs_log['conv_cnt'], dict_brs_log['conv_amnt'],
                                                    dict_row['session'], dict_row['tot_new_session'],
                                                    dict_row['tot_bounce'], dict_row['tot_duration_sec'],
                                                    dict_row['tot_pvs'], dict_row['tot_transactions'],
                                                    dict_row['tot_revenue'], dict_row['tot_registrations'],
                                                    self.__g_sDate)
                else:
                    # aggregate each log into a single GA detected NVAD log,
                    # if there happens duplicated nvad log by different nvad campaign name;
                    # GA recognize utm_campaign only, which is same level of NVAD adgrp name
                    self._print_debug('aggregate duplicated nvad log')
                    self._print_debug(self.__g_sDate)
                    self._print_debug(s_unique_tag)
                    self._print_debug(lst_nvad_log_daily)
                    n_cost = 0
                    n_imp = 0
                    n_click = 0
                    n_conv_cnt = 0
                    n_conv_amnt = 0
                    for dictSingleNvadLog in lst_nvad_log_daily:
                        n_cost += dictSingleNvadLog['cost']
                        n_imp += dictSingleNvadLog['imp']
                        n_click += dictSingleNvadLog['click']
                        n_conv_cnt += dictSingleNvadLog['conv_cnt']
                        n_conv_amnt += dictSingleNvadLog['conv_amnt']
                        dict_nvad_daily_log.pop(dictSingleNvadLog['log_srl'])
                    self.__g_oAgencyInfo.load_by_source_id(self.__g_sSvAcctId, self.__g_sBrandId,
                                                           'naver_ad', lst_nvad_log_daily[0]['customer_id'])
                    dict_cost_rst = self.__g_oAgencyInfo.get_cost_info('naver_ad',
                                                                       lst_nvad_log_daily[0]['customer_id'],
                                                                       self.__g_sDate, n_cost)
                    self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', dict_id_rst['s_ua'],
                                                    dict_id_rst['s_term'], 'naver', dict_id_rst['s_rst_type'],
                                                    dict_cost_rst['agency_id'], dict_cost_rst['agency_name'], dict_id_rst['s_media'],
                                                    dict_id_rst['s_brd'], dict_id_rst['s_camp_1st'],
                                                    dict_id_rst['s_camp_2nd'], dict_id_rst['s_camp_3rd'],
                                                    dict_cost_rst['cost'], dict_cost_rst['agency_fee'], dict_cost_rst['vat'],
                                                    n_imp, n_click, n_conv_cnt, n_conv_amnt, dict_row['session'],
                                                    dict_row['tot_new_session'], dict_row['tot_bounce'],
                                                    dict_row['tot_duration_sec'], dict_row['tot_pvs'],
                                                    dict_row['tot_transactions'], dict_row['tot_revenue'],
                                                    dict_row['tot_registrations'], self.__g_sDate)
        # means mob_brded':0, 'mob_nonbrded':0, 'pc_brded':0, 'pc_nonbrded
        dict_imp = {'M_1': 0, 'M_0': 0, 'P_1': 0, 'P_0': 0}
        for n_log_srl in dict_nvad_daily_log:  # register residual; means NVAD data without GA log
            if dict_nvad_daily_log[n_log_srl]['cost'] > 0 or dict_nvad_daily_log[n_log_srl]['campaign_1st'] == 'BRS':
                self.__g_oAgencyInfo.load_by_source_id(self.__g_sSvAcctId, self.__g_sBrandId, 'naver_ad',
                                                       dict_nvad_daily_log[n_log_srl]['customer_id'])
                dict_cost_rst = self.__g_oAgencyInfo.get_cost_info('naver_ad',
                                                                   dict_nvad_daily_log[n_log_srl]['customer_id'],
                                                                   self.__g_sDate,
                                                                   dict_nvad_daily_log[n_log_srl]['cost'])
                self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', dict_nvad_daily_log[n_log_srl]['ua'],
                                                dict_nvad_daily_log[n_log_srl]['term'], 'naver',
                                                dict_nvad_daily_log[n_log_srl]['rst_type'],
                                                dict_cost_rst['agency_id'], dict_cost_rst['agency_name'],
                                                dict_nvad_daily_log[n_log_srl]['media'],
                                                dict_nvad_daily_log[n_log_srl]['brd'],
                                                dict_nvad_daily_log[n_log_srl]['campaign_1st'],
                                                dict_nvad_daily_log[n_log_srl]['campaign_2nd'],
                                                dict_nvad_daily_log[n_log_srl]['campaign_3rd'],
                                                dict_cost_rst['cost'], dict_cost_rst['agency_fee'], dict_cost_rst['vat'],
                                                dict_nvad_daily_log[n_log_srl]['imp'],
                                                dict_nvad_daily_log[n_log_srl]['click'],
                                                dict_nvad_daily_log[n_log_srl]['conv_cnt'],
                                                dict_nvad_daily_log[n_log_srl]['conv_amnt'],
                                                0, 0, 0, 0, 0, 0, 0, 0, self.__g_sDate)
            elif dict_nvad_daily_log[n_log_srl]['cost'] == 0:
                s_indication = dict_nvad_daily_log[n_log_srl]['ua'] + '_' + str(dict_nvad_daily_log[n_log_srl]['brd'])
                dict_imp[s_indication] = dict_imp[s_indication] + int(dict_nvad_daily_log[n_log_srl]['imp'])

        for s_idx in dict_imp:
            if dict_imp[s_idx] > 0:
                a_idx = s_idx.split('_')
                s_ua = a_idx[0]
                b_brded = a_idx[1]
                self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', s_ua, '|@|sv', 'naver', 'PS', 0, '', 'cpc',
                                                b_brded, '|@|sv', '|@|sv', '|@|sv', 0, 0, 0, dict_imp[s_idx],
                                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, self.__g_sDate)

    def __merge_other_log(self):
        """
        PNS cost는 GA log 기준으로 배분할 수 없음
        impression은 애초에 알 수 없고,
        click은 계약 기간 중에 발생한다고 보장할 수 없고, 기간 후에 발생하지 않는다고 보장할 수 없음.
        """
        for s_unique_tag, dict_row in self.__g_dictOtherMergedDailyLog.items():
            dict_id_rst = self.__parse_uniq_log_id(s_unique_tag)
            n_media_raw_cost = 0
            n_media_agency_cost = 0
            n_media_cost_vat = 0
            self.__g_oSvMysql.execute_query('insertCompiledGaMediaDailyLog', dict_id_rst['s_ua'], dict_id_rst['s_term'],
                                            dict_id_rst['s_source'], dict_id_rst['s_rst_type'], 0, '', dict_id_rst['s_media'],
                                            dict_id_rst['s_brd'], dict_id_rst['s_camp_1st'], dict_id_rst['s_camp_2nd'],
                                            dict_id_rst['s_camp_3rd'], n_media_raw_cost,
                                            n_media_agency_cost, n_media_cost_vat, 0, 0, 0, 0,
                                            dict_row['session'], dict_row['tot_new_session'], dict_row['tot_bounce'],
                                            dict_row['tot_duration_sec'], dict_row['tot_pvs'],
                                            dict_row['tot_transactions'], dict_row['tot_revenue'],
                                            dict_row['tot_registrations'], self.__g_sDate)


if __name__ == '__main__':  # for console debugging
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with svJobPlugin() as oJob:  # to enforce to call plugin destructor
            oJob.set_my_name('integrate_db')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] params are required for console execution.')
