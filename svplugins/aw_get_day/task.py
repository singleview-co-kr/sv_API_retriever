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
# from datetime import date
from datetime import datetime
from datetime import timedelta
import time
import os
import csv
import sys

# 3rd party library
import google.auth.exceptions  # to catch google.auth.exceptions.RefreshError
from google.ads.googleads.v12.enums.types.device import DeviceEnum
from google.ads.googleads.client import GoogleAdsClient

# https://developers.google.com/google-ads/api/fields/v6/segments
# https://developers.google.com/google-ads/api/docs/query/overview
# cd /usr/local/lib/python3.7/site-packages/google/ads/googleads/v6/enums

# singleview library
if __name__ == '__main__':  # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svdjango')
    import sv_object
    import sv_plugin
    import settings
else:
    from svcommon import sv_object
    from svcommon import sv_plugin
    from django.conf import settings


class SvJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_sGoogleAdsApiVersion = 'v13'

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name + '(20230914)')
        # Declaring a dict outside __init__ is declaring a class-level variable.
        # It is only created once at first,
        # whenever you create new objects it will reuse this same dict.
        # To create instance variables, you declare them with self in __init__.

    def __del__(self):
        """ never place self._task_post_proc() here
            __del__() is not executed if try except occurred """
        pass

    def do_task(self, o_callback):
        self._g_oCallback = o_callback

        dict_acct_info = self._task_pre_proc(o_callback)

        if 'sv_account_id' not in dict_acct_info and 'brand_id' not in dict_acct_info:
            self._print_debug('stop -> invalid config_loc')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        if 'adw_cid' not in dict_acct_info:
            self._print_debug('stop -> no google ads API info')
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
            self._print_debug(('There was an error in constructing your query : %s' % error))

        self._task_post_proc(self._g_oCallback)

    def __get_adwords_raw(self, s_sv_acct_id, s_acct_title, s_googleads_cid):
        s_download_path = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_sv_acct_id, s_acct_title,
                                       'adwords', s_googleads_cid, 'data')
        if not os.path.isdir(s_download_path):
            os.makedirs(s_download_path)

        s_conf_path_abs = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_sv_acct_id, s_acct_title,
                                       'adwords', s_googleads_cid, 'conf')
        if not os.path.isdir(s_conf_path_abs):
            os.makedirs(s_conf_path_abs)

        s_google_ads_yaml_path = os.path.join(self._g_sAbsRootPath, 'conf', 'google-ads.yaml')
        try:
            o_googleads_client = GoogleAdsClient.load_from_storage(s_google_ads_yaml_path,
                                                                   version=self.__g_sGoogleAdsApiVersion)
        except google.auth.exceptions.RefreshError:
            self._print_debug('A refresh token in google-ads.yaml has expired!')
            self._print_debug('Run svinitialize/generate_user_credentials.py to get valid token.')
            return
        o_googleads_service = o_googleads_client.get_service("GoogleAdsService")

        s_latest_filepath = os.path.join(s_conf_path_abs, 'general.latest')
        if os.path.isfile(s_latest_filepath):
            f = open(s_latest_filepath, 'r')
            s_max_report_date = f.readline()
            dt_start_retrieval = datetime.strptime(s_max_report_date, '%Y%m%d') + timedelta(days=1)
            f.close()
        else:
            dt_start_retrieval = datetime.now() - timedelta(days=1)
        self._print_debug(s_googleads_cid + ' -> start date :' + dt_start_retrieval.strftime('%Y-%m-%d'))

        # requested report date should not be later than today
        dt_yesterday = datetime.now() - timedelta(days=1)
        dt_date_end_retrieval = dt_yesterday
        dt_date_diff = dt_date_end_retrieval - dt_start_retrieval
        n_num_days = int(dt_date_diff.days) + 1
        s_google_ads_cid = s_googleads_cid.replace('-', '')
        
        dict_date_queue = {}
        lst_effective_yrmo = []
        n_start_yyyymm = int(dt_start_retrieval.strftime('%Y%m'))
        n_yesterday_yyyymm = int(dt_yesterday.strftime('%Y%m'))
        if n_num_days > 10 and n_start_yyyymm < n_yesterday_yyyymm:  # bypass long resting period
            self._print_debug('remove resting days')
            dict_resting_yr_mo = {}
            for x in range(0, n_num_days):
                dt_element = dt_start_retrieval + timedelta(days=x)
                s_yrmo = dt_element.strftime('%Y%m')
                if s_yrmo not in dict_resting_yr_mo:
                    dict_resting_yr_mo[dt_element.strftime('%Y%m')] = []
                dict_resting_yr_mo[dt_element.strftime('%Y%m')].append(dt_element.day)

            for s_yrmo, lst_days in dict_resting_yr_mo.items():
                s_check_query = """SELECT
                                        metrics.cost_micros
                                    FROM campaign
                                    WHERE metrics.cost_micros > 0 AND segments.date >= """ + \
                                s_yrmo+'{0:02d}'.format(lst_days[0]) + " AND segments.date <= " + \
                                s_yrmo+'{0:02d}'.format(lst_days[-1])
                try:  # Issues a search request using streaming.
                    o_check_resp = o_googleads_service.search_stream(customer_id=s_google_ads_cid,
                                                                             query=s_check_query)
                except Exception as e:
                    self._print_debug('unknown exception occured while access googleads API')
                    self._print_debug(e)
                    if self._g_bDaemonEnv:  # for running on dbs.py only
                        raise Exception('remove')
                    else:
                        return
                n_cost = 0
                for disp_check_batch in o_check_resp:
                    for o_check_row in disp_check_batch.results:
                        n_cost += o_check_row.metrics.cost_micros
                if n_cost:
                    lst_effective_yrmo.append((s_yrmo+'{0:02d}'.format(lst_days[0]), s_yrmo+'{0:02d}'.format(lst_days[-1])))
                else:  # update general.latest for CID
                    try:
                        f = open(s_latest_filepath, 'w')
                        f.write(s_yrmo+'{0:02d}'.format(lst_days[-1]))
                        f.close()
                    except PermissionError:
                        if self._g_bDaemonEnv:  # for running on dbs.py only
                            raise Exception('remove')
                        else:
                            return
        
            for tup_month in lst_effective_yrmo:
                dt_start = datetime.strptime(tup_month[0], '%Y%m%d')
                dt_end = datetime.strptime(tup_month[-1], '%Y%m%d')
                dict_date_queue[dt_start] = 0

                while self._continue_iteration():
                    dt_start = dt_start + timedelta(days=1)
                    dict_date_queue[dt_start] = 0
                    if dt_start == dt_end:
                        break
        else:  # daily retrieving
            for x in range(0, n_num_days):
                dt_element = dt_start_retrieval + timedelta(days=x)
                dict_date_queue[dt_element] = 0

        if len(dict_date_queue) == 0:
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('stop')
            else:
                return

        # set device dictionary
        dict_googleads_v12_device = {i.value: i.name for i in DeviceEnum.Device}

        # set report header rows
        lst_report_header_1 = ['google_ads_api (' + self.__g_sGoogleAdsApiVersion + ')']
        lst_report_header_2 = ['Campaign', 'Ad group', 'Keyword / Placement', 'Impressions', 'Clicks', 'Cost', 'Device',
                               'Conversions', 'Total conv. value', 'Day']

        # https://developers.google.com/google-ads/api/fields/v12/query_validator
        while self._continue_iteration():  # loop for each report date
            try:  # find unhandled report task
                dt_retrieval = list(dict_date_queue.keys())[list(dict_date_queue.values()).index(0)]
            except ValueError:
                break

            s_data_date_for_mysql = dt_retrieval.strftime('%Y%m%d')
            s_tsv_filename = s_data_date_for_mysql + '_general.tsv'
            self._print_debug('--> ' + s_googleads_cid + ' will retrieve general report on ' + s_data_date_for_mysql)
            # notice! this GAQL query does not retrieve OFF campaign
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
                self._print_debug('unknown exception occured while access googleads API')
                self._print_debug(e)
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
                            self._print_debug('unknown exception occured while access googleads API')
                            self._print_debug(e)
                            if self._g_bDaemonEnv:  # for running on dbs.py only
                                raise Exception('stop')
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
                                dict_disp_campaign['Device'] = dict_googleads_v12_device[o_disp_campaign_row.segments.device]
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
                        dict_disp_campaign['Device'] = dict_googleads_v12_device[o_disp_campaign_row.segments.device]
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

            try:
                f = open(s_latest_filepath, 'w')
                f.write(s_data_date_for_mysql)
                f.close()
            except PermissionError:
                if self._g_bDaemonEnv:  # for running on dbs.py only
                    raise Exception('remove')
                else:
                    return
            dict_date_queue[dt_retrieval] = 1
            time.sleep(1)


if __name__ == '__main__':  # for console debugging and execution
    # python task.py config_loc=1/1
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with SvJobPlugin() as oJob:  # to enforce to call plugin destructor
            oJob.set_my_name('aw_get_day')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] params are required for console execution.')
