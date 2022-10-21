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
import sys
import logging
from datetime import datetime, timedelta
import time
import os
import csv
import calendar

# 3rd party library
import google.auth.exceptions  # to catch google.auth.exceptions.RefreshError
from google.ads.googleads.v10.enums.types.device import DeviceEnum
from google.ads.googleads.client import GoogleAdsClient

# https://developers.google.com/google-ads/api/fields/v6/segments
# https://developers.google.com/google-ads/api/docs/query/overview
# cd /usr/local/lib/python3.7/site-packages/google/ads/googleads/v6/enums

# singleview library
if __name__ == '__main__':  # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svdjango')
    import sv_mysql
    import sv_object
    import sv_campaign_parser
    import sv_plugin
    import settings
else:  # for platform running
    from svcommon import sv_mysql
    from svcommon import sv_object
    from svcommon import sv_campaign_parser
    from svcommon import sv_plugin
    from django.conf import settings


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_sGoogleAdsApiVersion = 'v10'

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name + '(20221021)')

        self._g_dictParam.update({'yyyymm': None})
        # Declaring a dict outside __init__ is declaring a class-level variable.
        # It is only created once at first,
        # whenever you create new objects it will reuse this same dict.
        # To create instance variables, you declare them with self in __init__.
        self.__g_sRetrieveMonth = None
        self.__g_lstDateQueue = []

    def __del__(self):
        """ never place self._task_post_proc() here
            __del__() is not executed if try except occurred """
        self.__g_sRetrieveMonth = None

    def do_task(self, o_callback):
        self._g_oCallback = o_callback

        self.__g_sRetrieveMonth = self._g_dictParam['yyyymm']

        dict_acct_info = self._task_pre_proc(o_callback)

        self.__get_retrieval_period(dict_acct_info['tbl_prefix'])
        if len(self.__g_lstDateQueue) == 0:
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('stop')
            else:
                self._printDebug('nothing to retreive')
                return
        
        if 'sv_account_id' not in dict_acct_info and 'brand_id' not in dict_acct_info:
            self._printDebug('stop -> invalid config_loc')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        if 'adw_cid' not in dict_acct_info:
            self._printDebug('stop -> no google ads API info')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return

        s_sv_acct_id = dict_acct_info['sv_account_id']
        s_brand_id = dict_acct_info['brand_id']
        lst_google_ads = dict_acct_info['adw_cid']
        try:
            for s_googleads_cid in lst_google_ads:
                self.__get_adwords_raw(s_sv_acct_id, s_brand_id, s_googleads_cid)
        except TypeError as error:
            # Handle errors in constructing a query.
            self._printDebug(('There was an error in constructing your query : %s' % error))

        self._task_post_proc(self._g_oCallback)

    def __get_retrieval_period(self, s_tbl_prefix):
        n_yr = int(self.__g_sRetrieveMonth[:4])
        n_mo = int(self.__g_sRetrieveMonth[4:None])
        try:
            lst_month_range = calendar.monthrange(n_yr, n_mo)
        except calendar.IllegalMonthError:
            self._printDebug('invalid yyyymm')
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return

        lst_google_ads_sm_id = []
        o_sv_campaign_parser = sv_campaign_parser.SvCampaignParser()
        dict_source_medium_type = o_sv_campaign_parser.get_source_medium_type_dict()
        for n_idx, dict_single_sm in dict_source_medium_type.items():
            if dict_single_sm['media_source'] == 'google' or dict_single_sm['media_source'] == 'youtube':
                lst_google_ads_sm_id.append(n_idx)
        del dict_source_medium_type

        # begin - get budget list for designated month
        s_start_date_retrieval = self.__g_sRetrieveMonth + '01'
        s_end_date_retrieval = self.__g_sRetrieveMonth + str(lst_month_range[1])
        with sv_mysql.SvMySql() as oSvMysql:
            oSvMysql.setTablePrefix(s_tbl_prefix)
            oSvMysql.set_app_name('svplugins.daily_cron')
            oSvMysql.initialize(self._g_dictSvAcctInfo)
            lst_rst = oSvMysql.executeQuery('getBudgetPeriodByPeriod', s_start_date_retrieval, s_end_date_retrieval)
        # end - get budget list for designated month

        lst_date_begin = []
        lst_date_end = []
        for dict_single in lst_rst:
            if dict_single['acct_id'] in lst_google_ads_sm_id:
                lst_date_begin.append(dict_single['date_begin'])
                lst_date_end.append(dict_single['date_end'])
        del lst_rst

        dt_start_retrieval = min(lst_date_begin)
        dt_date_end_retrieval = max(lst_date_end)
        dt_date_diff = dt_date_end_retrieval - dt_start_retrieval
        n_num_days = int(dt_date_diff.days) + 1
        for x in range(0, n_num_days):
            self.__g_lstDateQueue.append(dt_start_retrieval + timedelta(days=x))
        del lst_date_begin
        del lst_date_end
        del dt_start_retrieval
        del dt_date_end_retrieval
        del dt_date_diff

    def __get_adwords_raw(self, s_sv_acct_id, s_acct_title, s_adwords_cid):
        s_download_path = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_sv_acct_id, s_acct_title,
                                       'adwords', s_adwords_cid, 'data', 'closing')
        if os.path.isdir(s_download_path) is False:
            os.makedirs(s_download_path)

        # https://developers.google.com/adwords/api/docs/guides/accounts-overview?hl=ko#test_accounts
        # https://medium.com/@MihaZelnik/how-to-create-test-account-for-adwords-api-503ca72b25a4
        # MCC = My Customer Center
        # https://developers.google.com/adwords/api/docs/
        # https://github.com/googleads/googleads-python-lib
        # https://github.com/googleads/googleads-python-lib/releases
        # https://www.youtube.com/watch?v=80KOeuCNc0c
        s_google_ads_yaml_path = os.path.join(self._g_sAbsRootPath, 'conf', 'google-ads.yaml')
        
        try:
            o_googleads_client = GoogleAdsClient.load_from_storage(s_google_ads_yaml_path,
                                                                   version=self.__g_sGoogleAdsApiVersion)
        except google.auth.exceptions.RefreshError:
            self._printDebug('A refresh token in google-ads.yaml has expired!')
            self._printDebug('Run svinitialize/generate_user_credentials.py to get valid token.')
            return
        o_googleads_service = o_googleads_client.get_service('GoogleAdsService')
        dict_date_queue = dict()
        for dt_single in self.__g_lstDateQueue:
            dict_date_queue.update({dt_single: 0})

        # set device dictionary
        dict_googleads_v10_device = {i.value: i.name for i in DeviceEnum.Device}
        s_google_ads_cid = s_adwords_cid.replace('-', '')

        # set report header rows
        lst_report_header_1 = ['google_ads_api (' + self.__g_sGoogleAdsApiVersion + ')']
        lst_report_header_2 = ['Campaign', 'Ad group', 'Keyword / Placement', 'Impressions', 'Clicks', 'Cost', 'Device',
                               'Conversions', 'Total conv. value', 'Day']
        while self._continue_iteration():  # loop for each report date
            try:
                # find unhandled report task
                dt_retrieval = list(dict_date_queue.keys())[list(dict_date_queue.values()).index(0)]
                s_data_date_for_mysql = dt_retrieval.strftime('%Y%m%d')
                self._printDebug('--> ' + s_adwords_cid + ' will retrieve general report on ' + s_data_date_for_mysql)
            except ValueError:
                break

            s_tsv_filename = s_data_date_for_mysql + '_general.tsv'
            s_disp_campaign_query = """
                    SELECT
                        campaign.id,
                        campaign.name,
                        metrics.impressions, 
                        metrics.clicks, 
                        metrics.cost_micros, 
                        segments.device, 
                        metrics.all_conversions, 
                        metrics.all_conversions_value, 
                        segments.date 
                    FROM campaign
                    WHERE metrics.cost_micros > 0 AND segments.date = """ + s_data_date_for_mysql
            try:  # Issues a search request using streaming.
                o_disp_campaign_resp = o_googleads_service.search_stream(customer_id=s_google_ads_cid,
                                                                         query=s_disp_campaign_query)
            except Exception as e:
                self._printDebug('unknown exception occurred while access googleads API')
                self._printDebug(e)
                if self._g_bDaemonEnv:  # for running on dbs.py only
                    raise Exception('remove')
                else:
                    return

            lst_logs = []
            for disp_campaign_batch in o_disp_campaign_resp:
                for o_disp_campaign_row in disp_campaign_batch.results:
                    dict_disp_campaign = {'CampaignName': None, 'AdGroupName': None, 'Criteria': None, 'Impressions': 0,
                                          'Clicks': 0, 'Cost': 0,
                                          'Device': None, 'Conversions': 0, 'ConversionValue': 0, 'Date': None}
                    lst_campaign_code = o_disp_campaign_row.campaign.name.split('_')
                    if lst_campaign_code[2] == 'CPC' and lst_campaign_code[3] != 'GDN':  # search term campaign
                        s_text_campaign_query = """
                            SELECT
                                campaign.name,
                                ad_group_criterion.keyword.text
                                metrics.clicks, 
                                metrics.cost_micros, 
                                metrics.all_conversions, 
                                metrics.all_conversions_value, 
                                segments.date 
                            FROM search_term_view
                            WHERE segments.date = """ + s_data_date_for_mysql + ' AND ' + \
                                                'campaign.id = ' + str(o_disp_campaign_row.campaign.id)
                        try:  # Issues a search request using streaming.
                            o_txt_campaign_resp = o_googleads_service.search_stream(customer_id=s_google_ads_cid,
                                                                                    query=s_text_campaign_query)
                        except Exception as e:
                            self._printDebug('unknown exception occured while access googleads API')
                            self._printDebug(e)
                            if self._g_bDaemonEnv:  # for running on dbs.py only
                                raise Exception('remove')
                            else:
                                return
                        for txt_campaign_batch in o_txt_campaign_resp:
                            for o_txt_campaign_row in txt_campaign_batch.results:
                                dict_disp_campaign['CampaignName'] = o_disp_campaign_row.campaign.name
                                dict_disp_campaign['AdGroupName'] = 'n/a'
                                dict_disp_campaign['Criteria'] = o_txt_campaign_row.ad_group_criterion.keyword.text
                                # refer to o_disp_campaign_row because [search_term_view] does not provide
                                dict_disp_campaign['Impressions'] = o_disp_campaign_row.metrics.impressions
                                dict_disp_campaign['Clicks'] = o_txt_campaign_row.metrics.clicks
                                dict_disp_campaign['Cost'] = o_txt_campaign_row.metrics.cost_micros
                                # refer to o_disp_campaign_row because [search_term_view] does not provide
                                dict_disp_campaign['Device'] = dict_googleads_v10_device[o_disp_campaign_row.segments.device]
                                dict_disp_campaign['Conversions'] = o_txt_campaign_row.metrics.all_conversions
                                dict_disp_campaign['ConversionValue'] = o_txt_campaign_row.metrics.all_conversions_value
                                dict_disp_campaign['Date'] = o_txt_campaign_row.segments.date
                                lst_logs.append(dict_disp_campaign)
                        del o_txt_campaign_resp
                    else:  # display campaign
                        dict_disp_campaign['CampaignName'] = o_disp_campaign_row.campaign.name
                        dict_disp_campaign['AdGroupName'] = 'n/a'
                        dict_disp_campaign['Criteria'] = 'AutomaticContent'
                        dict_disp_campaign['Impressions'] = o_disp_campaign_row.metrics.impressions
                        dict_disp_campaign['Clicks'] = o_disp_campaign_row.metrics.clicks
                        dict_disp_campaign['Cost'] = o_disp_campaign_row.metrics.cost_micros
                        dict_disp_campaign['Device'] = dict_googleads_v10_device[o_disp_campaign_row.segments.device]
                        dict_disp_campaign['Conversions'] = o_disp_campaign_row.metrics.all_conversions
                        dict_disp_campaign['ConversionValue'] = o_disp_campaign_row.metrics.all_conversions_value
                        dict_disp_campaign['Date'] = o_disp_campaign_row.segments.date
                        lst_logs.append(dict_disp_campaign)
            # write data stream to file.
            with open(os.path.join(s_download_path, s_tsv_filename), 'w', encoding='utf-8') as out:
                wr = csv.writer(out, delimiter='\t')
                wr.writerow(lst_report_header_1)
                wr.writerow(lst_report_header_2)
                for dict_rows in lst_logs:
                    wr.writerow(list(dict_rows.values()))
            dict_date_queue[dt_retrieval] = 1
            time.sleep(3)


if __name__ == '__main__':  # for console debugging and execution
    # python task.py config_loc=1/1 yyyymm=202109
    nCliParams = len(sys.argv)
    if nCliParams > 2:
        with svJobPlugin() as oJob:  # to enforce to call plugin destructor
            oJob.set_my_name('aw_get_month')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] [yyyymm] params are required for console execution.')
