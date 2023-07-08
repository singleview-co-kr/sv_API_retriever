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
from datetime import datetime
from datetime import timedelta
import time
import os
import sys
import random

# 3rd party library
from decouple import config  # https://pypi.org/project/python-decouple/

# for GA4 only
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Filter,
    FilterExpression,
    # FilterExpressionList,
    Metric,
    OrderBy,
    RunReportRequest,
)

# For Universal Analytics only
from apiclient.discovery import build
from oauth2client import file
from oauth2client import client
from oauth2client import tools
from googleapiclient.errors import HttpError
from oauth2client.client import AccessTokenRefreshError


# singleview library
if __name__ == '__main__': # for console debugging
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
    __g_sAbsRootPath = config('ABSOLUTE_PATH_BOT')

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name+'(20230704)')
        self._g_oLogger.setLevel(logging.ERROR)

        # log format
        formatter = logging.Formatter('%(name)s [%(levelname)s] %(message)s @ %(asctime)s')
        # log output format
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        self._g_oLogger.addHandler(stream_handler)
        # log output to file stream
        file_handler = logging.FileHandler(self.__g_sAbsRootPath + '/log/sv_campaign_parser.' + str(datetime.today().strftime('%Y%m%d')) + '.log')
        file_handler.setFormatter(formatter)
        self._g_oLogger.addHandler(file_handler)

        # Declaring a dict outside __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        self.__g_lstAccessLevel = []
    
    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        self.__g_lstAccessLevel = []

    def do_task(self, o_callback):
        self._g_oCallback = o_callback
        
        dict_acct_info = self._task_pre_proc(o_callback)
       
        if 'sv_account_id' not in dict_acct_info and 'brand_id' not in dict_acct_info and \
          'google_analytics' not in dict_acct_info:
            self._print_debug('stop -> invalid config_loc')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        s_sv_acct_id = dict_acct_info['sv_account_id']
        s_brand_id = dict_acct_info['brand_id']
        s_version = dict_acct_info['google_analytics']['s_version']
        s_property_or_view_id = dict_acct_info['google_analytics']['s_property_or_view_id']
        self.__g_lstAccessLevel = dict_acct_info['google_analytics']['lst_access_level']
        if s_version == 'ua':  # universal analytics
            """ Authenticate and construct service. """
            # Define the auth scopes to request.
            scope = ['https://www.googleapis.com/auth/analytics.readonly']
            # Authenticate and construct service.
            sPrivateKeyJson = os.path.join(self._g_sAbsRootPath, 'conf', 'private_key_google_analytics.json')
            service = self.__get_service_oauth2('analytics', 'v3', scope, sPrivateKeyJson)
            # Try to make a request to the API. Print the results or handle errors. 
            try:
                self.__get_insite_raw_ua(service, s_sv_acct_id, s_brand_id, s_property_or_view_id)
            except TypeError as error:
                # Handle errors in constructing a query.
                self._print_debug(('There was an error in constructing your query : %s' % error))
                if self._g_bDaemonEnv:  # for running on dbs.py only
                    raise Exception('remove')
                else:
                    return
            except HttpError as error:
                # Handle API errors.
                self._print_debug(('Arg, there was an API error : %s : %s' % (error.resp.status, error._get_reason())))
                if self._g_bDaemonEnv:  # for running on dbs.py only
                    raise Exception('remove')
                else:
                    return
            except AccessTokenRefreshError:
                # Handle Auth errors.
                self._print_debug('The credentials have been revoked or expired, please re-run the application to re-authorize')
                if self._g_bDaemonEnv:  # for running on dbs.py only
                    raise Exception('remove')
                else:
                    return
        elif s_version == 'ga4':
            self._print_debug('choose ga4_get_day plugin')
            try:
                self.__get_insite_raw_ga4(s_sv_acct_id, s_brand_id, s_property_or_view_id)
            except TypeError as error:
                # Handle errors in constructing a query.
                self._print_debug(('There was an error in constructing your query : %s' % error))
                if self._g_bDaemonEnv:  # for running on dbs.py only
                    raise Exception('remove')
                else:
                    return

        self._task_post_proc(self._g_oCallback)

    def __get_insite_raw_ga4(self, s_sv_acct_id, s_acct_title, s_ga4_property_id):
        s_data_path = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_sv_acct_id, s_acct_title,
                                   'google_analytics', s_ga4_property_id, 'data')
        if os.path.isdir(s_data_path) is False:
            os.makedirs(s_data_path)
        s_conf_path = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_sv_acct_id, s_acct_title,
                                   'google_analytics', s_ga4_property_id, 'conf')
        if os.path.isdir(s_conf_path) is False:
            os.makedirs(s_conf_path)

        # https://developers.google.com/analytics/devguides/reporting/data/v1/quickstart-client-libraries  * most important page: contains available met dim list
        # https://github.com/googleapis/python-analytics-data
        # https://googleapis.dev/python/analyticsdata/latest/data_v1beta/beta_analytics_data.html
        lst_to_query = []
        if 'homepage' in self.__g_lstAccessLevel:
            lst_to_query.append({'met': ['sessions'],
                                 'dim': ['firstUserSourceMedium', 'firstUserCampaignName', 'firstUserManualTerm'],
                                 'filter': ['firstUserSourceMedium']})
            lst_to_query.append({'met': ['bounceRate'],
                                 'dim': ['firstUserSourceMedium', 'firstUserCampaignName', 'firstUserManualTerm'],
                                 'filter': ['firstUserSourceMedium']})
            # UA 100이 최대치 GA4는 1이 최대치
            lst_to_query.append({'met': ['newUsers'],
                                 'dim': ['firstUserSourceMedium', 'firstUserCampaignName', 'firstUserManualTerm'],
                                 'filter': ['firstUserSourceMedium']})
            # tsv 파일명이 다르고 UA는 % GA4는 절대값
            lst_to_query.append({'met': ['screenPageViewsPerSession'],
                                 'dim': ['firstUserSourceMedium', 'firstUserCampaignName', 'firstUserManualTerm'],
                                 'filter': ['firstUserSourceMedium'],
                                 'filename': 'pageviewsPerSession'})
            lst_to_query.append({'met': ['averageSessionDuration'],
                                 'dim': ['firstUserSourceMedium', 'firstUserCampaignName', 'firstUserManualTerm'],
                                 'filter': ['firstUserSourceMedium'],
                                 'filename': 'avgSessionDuration'})
        if 'internal_search' in self.__g_lstAccessLevel:
            lst_to_query.append({'met': ['sessions'],
                                 'dim': ['searchTerm'],
                                 'filter': ['searchTerm'],
                                 'filename': 'searchUniques'})  # simulate ua searchUniques and searchKeyword
        if 'catalog' in self.__g_lstAccessLevel:
            lst_to_query.append({'met': ['itemsViewedInList'],
                                 'dim': ['itemName'],
                                 'filter': ['itemName'],
                                 'filename': 'productListViews'})
            lst_to_query.append({'met': ['itemsClickedInList'],
                                 'dim': ['itemName'],
                                 'filter': ['itemName'],
                                 'filename': 'productListClicks'})
            lst_to_query.append({'met': ['itemsViewed'],
                                 'dim': ['itemName'],
                                 'filter': ['itemName'],
                                 'filename': 'productDetailViews'})
        if 'payment' in self.__g_lstAccessLevel:
            # no need to query: can be calculated by other metrics
            # Unique purchases divided by views of product detail pages (Enhanced Ecommerce)
            # lst_to_query.append({'met': ['purchaseToViewRate'],
            #                      'dim': ['itemName'],
            #                      'filter': ['itemName'],
            #                      'filename': 'buyToDetailRate'})
            # Product adds divided by views of product details (Enhanced Ecommerce).
            # lst_to_query.append({'met': ['cartToViewRate'],
            #                      'dim': ['itemName'],
            #                      'filter': ['itemName'],
            #                      'filename': 'cartToDetailRate'})
            # Number of product units added to the shopping cart; was quantityAddedToCart for UA
            lst_to_query.append({'met': ['itemsAddedToCart'],
                                 'dim': ['itemName'],
                                 'filter': ['itemName'],
                                 'filename': 'productAddsToCart'})
            # --deprecated in GA4?- lst_to_query.append({'met': 'quantityAddedToCart', 'dim': 'ga:productName', 'sort':'ga:productName', 'filename':'quantityAddedToCart'})  # Number of product units added to the shopping cart
            # --deprecated in GA4?- lst_to_query.append({'met': 'productRemovesFromCart', 'dim': 'ga:productName', 'sort':'ga:productName', 'filename':'productRemovesFromCart'})
            # quota burden query; was productRevenuePerPurchase for UA
            lst_to_query.append({'met': ['itemRevenue'],
                                 'dim': ['itemName'],
                                 'filter': ['itemName'],
                                 'filename': 'productRevenuePerPurchase'})
            # Total number of items purchased. For example, if users purchase 2 frisbees and 5 tennis balls, this will be 7. was itemQuantity for UA
            lst_to_query.append({'met': ['itemsPurchased'], 
                                 'dim': ['itemName'], 
                                 'filter': ['itemName'],
                                 'filename': 'itemQuantity'})
            # Number of times the product was included in the check-out process (Enhanced Ecommerce). was productCheckouts for UA
            lst_to_query.append({'met': ['itemsCheckedOut'], 
                                 'dim': ['itemName'], 
                                 'filter': ['itemName'],
                                 'filename': 'productCheckouts'})
            # --deprecated in GA4?- lst_to_query.append({'met': 'quantityCheckedOut', 'dim': 'ga:productName', 'sort':'ga:productName', 'filename':'quantityCheckedOut'})  # Number of product units included in check out (Enhanced Ecommerce).
            lst_to_query.append({'met': ['transactions'],
                                 'dim': ['firstUserSourceMedium', 'firstUserCampaignName', 'firstUserManualTerm'],
                                 'filter': ['firstUserSourceMedium']})
            lst_to_query.append({'met': ['purchaseRevenue'],
                                 'dim': ['transactionId', 'firstUserSourceMedium', 'firstUserCampaignName',
                                         'firstUserManualTerm'], 
                                 'filter': ['firstUserSourceMedium'],
                                 'filename': 'transactionRevenueByTrId'})
        dict_ua_filter = {'PC': FilterExpression(
            filter=Filter(
                field_name="deviceCategory",
                string_filter=Filter.StringFilter(value="Desktop"),
            )
        ),
            'MOB': FilterExpression(
                not_expression=FilterExpression(
                    filter=Filter(
                        field_name="deviceCategory",
                        string_filter=Filter.StringFilter(value="Desktop"),
                    )
                )
            )
        }
        s_private_key_json = os.path.join(self._g_sAbsRootPath, 'conf', 'private_key_google_analytics.json')
        o_client = BetaAnalyticsDataClient.from_service_account_file(s_private_key_json)
        n_backoff_cnt = 0
        for s_ua, o_dim_filter in dict_ua_filter.items():
            for dict_setting in lst_to_query:
                if 'filename' in dict_setting:  # set designated download filename if requested, or follow metric name
                    s_filename = dict_setting['filename']
                else:
                    s_filename = '_'.join(dict_setting['met'])

                try:
                    s_latest_filepath = os.path.join(s_conf_path, s_ua + '_' + s_filename + '.latest')
                    f = open(s_latest_filepath, 'r')
                    s_max_report_date = f.readline()
                    dt_start_retrieval = datetime.strptime(s_max_report_date, '%Y%m%d') + timedelta(days=1)
                    f.close()
                except FileNotFoundError:
                    dt_start_retrieval = datetime.now() - timedelta(days=1)

                # requested report date should not be later than today
                dt_date_end_retrieval = datetime.now() - timedelta(days=1)  # yesterday
                dt_date_diff = dt_date_end_retrieval - dt_start_retrieval
                n_num_days = int(dt_date_diff.days) + 1
                dict_date = {}
                for x in range(0, n_num_days):
                    dt_element = dt_start_retrieval + timedelta(days=x)
                    dict_date[dt_element] = 0
                if len(dict_date) == 0:
                    continue

                lst_metrics = []
                lst_dimensions = []
                lst_filters = []
                lst_single_met = dict_setting['met']
                for s_single_met in lst_single_met:
                    lst_metrics.append(Metric(name=s_single_met))
                lst_single_dim = dict_setting['dim']
                for s_single_dim in lst_single_dim:
                    lst_dimensions.append(Dimension(name=s_single_dim))
                if 'filter' in dict_setting:
                    lst_single_filter = dict_setting['filter']
                    for s_single_filter in lst_single_filter:
                        lst_filters.append(OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name=s_single_filter), desc=True))

                while self._continue_iteration():  # loop for each report date
                    try:  # find unhandled report task
                        dt_retrieval = list(dict_date.keys())[list(dict_date.values()).index(0)]
                    except ValueError:
                        break

                    s_data_date_mysql = dt_retrieval.strftime('%Y%m%d')
                    self._print_debug('--> ' + s_ga4_property_id + ' retrieves ' + s_ua + '-' + s_filename \
                                      + ' report on ' + s_data_date_mysql)
                    s_data_date = dt_retrieval.strftime('%Y-%m-%d')
                    # write data table to file.
                    o_request = RunReportRequest(
                        property=f"properties/{s_ga4_property_id}",
                        dimensions=lst_dimensions,
                        metrics=lst_metrics,
                        dimension_filter=o_dim_filter,
                        order_bys=lst_filters,
                        date_ranges=[DateRange(start_date=s_data_date, end_date=s_data_date)],
                    )
                    try:
                        o_response = o_client.run_report(o_request)
                    
                    except Exception as e:
                        # if error.resp.reason in ['internalServerError', 'backendError']:
                        self._g_oLogger.error('504 deadline exceeded?')
                        self._g_oLogger.error(e)
                        self._print_debug(e)
                        if str(e).contains('504 deadline exceeded'):
                            if n_backoff_cnt < 5:
                                self._print_debug('start retrying with exponential back-off that GA recommends.')
                                self._print_debug(e.resp)
                                time.sleep((2 ** n_backoff_cnt) + random.random())
                                n_backoff_cnt += 1
                            else:
                                raise Exception('remove')
                                return
                        else:
                            s_msg = 'GA api has reported weird error while processing sv account id: ' + s_ga4_property_id
                            if self._g_bDaemonEnv:  # for running on dbs.py only
                                logging.info(e)
                                logging.info(s_msg)
                                raise Exception('remove' )
                            else:
                                self._print_debug(e)
                                self._print_debug(s_msg)
                                return
                    del o_request

                    n_total_rst = o_response.row_count
                    # dictResult = oRst.get('totalsForAllResults')
                    # n_total_for_all_rst = int(float(dictResult.get('ga:' + sMet )))
                    if n_total_rst > 0:
                        s_tsv_filename = s_data_date_mysql + '_' + s_ua + '_' + s_filename + '.tsv'
                        with open(os.path.join(s_data_path, s_tsv_filename), 'w', encoding='utf-8') as out:
                            for o_single_row in o_response.rows:
                                lst_dims = []
                                for lst_single_dim_val in o_single_row.dimension_values:
                                    if len(lst_single_dim_val.value):  # ignore no dimension value
                                        lst_dims.append(lst_single_dim_val.value.replace('"', '').replace("'", ''))

                                if len(lst_dims):
                                    out.write('\t'.join(lst_dims) + '\t' + \
                                            o_single_row.metric_values[0].value.replace('"', '').replace("'", ''))
                                    out.write('\n')
                                del lst_dims
                                del o_single_row

                    if o_response.property_quota:
                        self._print_debug('Tokens per day quota consumed: ' + 
                                          str(o_response.property_quota.tokens_per_day.consumed) + ', ')
                        self._print_debug('remaining ' + str(o_response.property_quota.tokens_per_day.remaining))
                        self._print_debug('Tokens per hour quota consumed: ' + 
                                          str(o_response.property_quota.tokens_per_hour.consumed) + ', ')
                        self._print_debug('remaining ' + str(o_response.property_quota.tokens_per_hour.remaining))
                        self._print_debug('Concurrent requests quota consumed: ' + 
                                          str(o_response.property_quota.concurrent_requests.consumed) + ', ')
                        self._print_debug('remaining ' + str(o_response.property_quota.concurrent_requests.remaining))
                        self._print_debug('Server errors per project per hour quota consumed: ' + 
                                          str(o_response.property_quota.server_errors_per_project_per_hour.consumed) + ', ')
                        self._print_debug('remaining ' + 
                                          str(o_response.property_quota.server_errors_per_project_per_hour.remaining))
                        self._print_debug('Potentially thresholded requests per hour quota consumed: ' + 
                                          str(o_response.property_quota.potentially_thresholded_requests_per_hour.consumed) + ', ')
                        self._print_debug('remaining ' + 
                                          str(o_response.property_quota.potentially_thresholded_requests_per_hour.remaining))
                        self._print_debug('remove')
                        return
                    del o_response
                    try:
                        f = open(s_latest_filepath, 'w')
                        f.write(s_data_date_mysql)
                        f.close()
                    except PermissionError:
                        self._print_debug(
                            'Permission error occurred while tag latest rpt date for sv account id: ' + s_sv_acct_id)
                        self._print_debug('remove')
                        return
                    dict_date[dt_retrieval] = 1
                    time.sleep(2)
                del lst_dimensions
                del lst_metrics
                del lst_filters
        del lst_to_query
        return
        
    def __get_insite_raw_ua(self, service, sSvAcctId, sAcctTitle, sGaViewId):
        """Executes and returns data from the Core Reporting API.
        This queries the API for the top 25 organic search terms by visits.
        Args:
        service: The service object built by the Google API Python client library.
        sGaViewId: String The profile ID from which to retrieve analytics data.
        Returns:
        The response returned from the Core Reporting API.
        """
        sDataPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, sSvAcctId, sAcctTitle, 'google_analytics', sGaViewId, 'data')
        if os.path.isdir(sDataPath) == False :
            os.makedirs(sDataPath)
        sConfPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, sSvAcctId, sAcctTitle, 'google_analytics', sGaViewId, 'conf')
        if os.path.isdir(sConfPath) == False :
            os.makedirs(sConfPath)
        
        # https://developers.google.com/analytics/devguides/reporting/core/dimsmets
        # https://ga-dev-tools.appspot.com/query-explorer/
        # dictation format : metric, dimension, sort, designated_filename
        lst_to_query = []
        if 'homepage' in self.__g_lstAccessLevel:
            lst_to_query.append({'met': 'sessions', 'dim': 'ga:sourceMedium, ga:campaign, ga:keyword', 'sort':'ga:sourceMedium'})
            lst_to_query.append({'met': 'bounceRate', 'dim': 'ga:sourceMedium, ga:campaign, ga:keyword', 'sort':'ga:sourceMedium'})
            lst_to_query.append({'met': 'percentNewSessions', 'dim': 'ga:sourceMedium, ga:campaign, ga:keyword', 'sort':'ga:sourceMedium'})
            lst_to_query.append({'met': 'pageviewsPerSession', 'dim': 'ga:sourceMedium, ga:campaign, ga:keyword', 'sort':'ga:sourceMedium'})
            lst_to_query.append({'met': 'avgSessionDuration', 'dim': 'ga:sourceMedium, ga:campaign, ga:keyword', 'sort':'ga:sourceMedium'})
        if 'internal_search' in self.__g_lstAccessLevel:
            lst_to_query.append({'met': 'searchUniques', 'dim': 'ga:searchKeyword', 'sort':'ga:searchKeyword'})
        if 'catalog' in self.__g_lstAccessLevel:  # 'dim': 'ga:productSku' <- sku id  vs 'dim': 'ga:productName' <- sku title
            lst_to_query.append({'met': 'productListViews', 'dim': 'ga:productName', 'sort':'ga:productName'})  # quota burden query
            lst_to_query.append({'met': 'productListClicks', 'dim': 'ga:productName', 'sort':'ga:productName'})  # quota burden query
            lst_to_query.append({'met': 'productDetailViews', 'dim': 'ga:productName', 'sort':'ga:productName'})  # quota burden query
        if 'payment' in self.__g_lstAccessLevel:
            # lst_to_query.append({'met': 'buyToDetailRate', 'dim': 'ga:productName', 'sort':'ga:productName'})  # Unique purchases divided by views of product detail pages (Enhanced Ecommerce)
            # lst_to_query.append({'met': 'cartToDetailRate', 'dim': 'ga:productName', 'sort':'ga:productName'})  # Product adds divided by views of product details (Enhanced Ecommerce).
            lst_to_query.append({'met': 'productAddsToCart', 'dim': 'ga:productName', 'sort':'ga:productName'})  # Number of times the product was added to the shopping cart
            lst_to_query.append({'met': 'quantityAddedToCart', 'dim': 'ga:productName', 'sort':'ga:productName'})  # Number of product units added to the shopping cart
            lst_to_query.append({'met': 'productRemovesFromCart', 'dim': 'ga:productName', 'sort':'ga:productName'})
            lst_to_query.append({'met': 'productRevenuePerPurchase', 'dim': 'ga:productName', 'sort':'ga:productName'})  # quota burden query; Average product revenue per purchase (commonly used with Product Coupon Code) (ga:itemRevenue / ga:uniquePurchases) - (Enhanced Ecommerce). This field is disallowed in segments.
            lst_to_query.append({'met': 'itemQuantity', 'dim': 'ga:productName', 'sort':'ga:productName'})  # Total number of items purchased. For example, if users purchase 2 frisbees and 5 tennis balls, this will be 7.
            lst_to_query.append({'met': 'productCheckouts', 'dim': 'ga:productName', 'sort':'ga:productName'})  # Number of times the product was included in the check-out process (Enhanced Ecommerce).
            lst_to_query.append({'met': 'quantityCheckedOut', 'dim': 'ga:productName', 'sort':'ga:productName'})  # Number of product units included in check out (Enhanced Ecommerce).
            lst_to_query.append({'met': 'transactions', 'dim': 'ga:sourceMedium, ga:campaign, ga:keyword', 'sort':'ga:sourceMedium'})
            lst_to_query.append({'met': 'transactionRevenue', 'dim': 'ga:transactionId, ga:sourceMedium, ga:campaign, ga:keyword', 'sort':'ga:sourceMedium', 'filename':'transactionRevenueByTrId'})

        nRetryBackoffCnt = 0
        dictInsiteUaSegment = {'PC': 'sessions::condition::ga:deviceCategory==desktop', 
            'MOB': 'sessions::condition::ga:deviceCategory==mobile,ga:deviceCategory==tablet'}
        for sUa in dictInsiteUaSegment:
            for dictSetting in lst_to_query:
                sMet = dictSetting['met']
                try: # set designated download filename if requested, or follow metric name
                    sFileName = dictSetting['filename']
                except KeyError:
                    sFileName = sMet
                try:
                    sLatestFilepath = os.path.join(sConfPath, sUa+'_'+sFileName+'.latest')
                    f = open(sLatestFilepath, 'r')
                    sMaxReportDate = f.readline()
                    dtStartRetrieval = datetime.strptime(sMaxReportDate, '%Y%m%d') + timedelta(days=1)
                    f.close()
                except FileNotFoundError:
                    dtStartRetrieval = datetime.now() - timedelta(days=1)

                # requested report date should not be later than today
                dtDateEndRetrieval = datetime.now() - timedelta(days=1) # yesterday
                dtDateDiff = dtDateEndRetrieval - dtStartRetrieval
                nNumDays = int(dtDateDiff.days ) + 1
                dictDateQueue = dict()
                for x in range (0, nNumDays):
                    dtElement = dtStartRetrieval + timedelta(days = x)
                    dictDateQueue[dtElement] = 0
                if len(dictDateQueue) == 0:
                    continue
                while self._continue_iteration(): # loop for each report date
                    try:
                        dtRetrieval = list(dictDateQueue.keys())[list(dictDateQueue.values()).index(0)] # find unhandled report task
                        sDataDate = dtRetrieval.strftime('%Y-%m-%d')
                        sDataDateForMysql = dtRetrieval.strftime('%Y%m%d')
                        self._print_debug( '--> '+ sGaViewId +' retrieves ' + sUa + '-' + sFileName + ' report on ' + sDataDateForMysql)
                        try:
                            oRst = service.data().ga().get(
                                ids='ga:' + sGaViewId,
                                start_date = sDataDate,
                                end_date = sDataDate,
                                dimensions = dictSetting['dim'], #'ga:sourceMedium, ga:campaign, ga:keyword',
                                metrics = 'ga:'+sMet,
                                segment = dictInsiteUaSegment[sUa],
                                sort = '-' + dictSetting['sort'], #'-ga:sourceMedium',
                                #filters='ga:medium==organic',
                                #start_index='1', 
                                # https://groups.google.com/g/google-analytics-data-export-api/c/_WwxMiD5OYM?pli=1
                                max_results='2000' # The &max-results value can be set to 10000. Default (unspecified) is 1000.
                                # If you want more than 10000, you need to paginate with multiple queries and join the results.
                                ).execute()
                            sTsvFilename = sDataDateForMysql + '_' + sUa + '_' + sFileName + '.tsv'
                            # write data table to file.
                            with open(os.path.join(sDataPath, sTsvFilename), 'w', encoding='utf-8') as out:
                                if oRst.get('rows', []):
                                    for row in oRst.get('rows'):
                                        for cell in row:
                                            out.write(cell.replace('"', '').replace("'", '') + '\t')
                                        out.write( '\n')
                            try:
                                f = open(sLatestFilepath, 'w')
                                f.write(sDataDateForMysql)
                                f.close()
                            except PermissionError:
                                break
                            dictDateQueue[dtRetrieval] = 1
                            time.sleep(1)
                        except HttpError as error:
                            # https://developers.google.com/analytics/devguides/reporting/core/v4/errors
                            if error.resp.reason in ['quotaExceeded']:
                                self._print_debug('stop - daily or monthly quota exceeded')
                                return
                            elif error.resp.reason in ['userRateLimitExceeded','internalServerError', 'backendError']:
                                if nRetryBackoffCnt < 5:
                                    self._print_debug('start retrying with exponential back-off that GA recommends.')
                                    self._print_debug(error.resp)
                                    time.sleep((2 ** nRetryBackoffCnt) + random.random())
                                    nRetryBackoffCnt = nRetryBackoffCnt + 1
                                else:
                                    raise Exception('remove')
                        except Exception as e:
                            self._print_debug(e)
                            self._print_debug('GA api has reported weird error while processing sv account id: ' + sSvAcctId)
                            self._print_debug('remove')  # raise Exception('remove' )
                            return
                    except ValueError:
                        break
    
    def __get_service_oauth2(self, api_name, api_version, scopes, key_file_location):
        """Get a service that communicates to a Google API.
        Args:
            api_name: The name of the api to connect to.
            api_version: The api version to connect to.
            scopes: A list auth scopes to authorize for the application.
            key_file_location: The path to a valid service account JSON key file.
        Returns:
            A service that is connected to the specified API.
        """
        from oauth2client.service_account import ServiceAccountCredentials  # regarding OOB auth will be deprecated
        credentials = ServiceAccountCredentials.from_json_keyfile_name(key_file_location, scopes=scopes)
        # Build the service object.
        service = build(api_name, api_version, credentials=credentials)
        return service

if __name__ == '__main__':  # for console debugging and execution
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        # if sys.argv[1] == '--noauth_local_webserver':
        #     print('noauth')
        #     with SvJobPlugin() as oJob:  # to enforce to call plugin destructor
        #     	oJob.getConsoleAuth( sys.argv )
        # else:
        with SvJobPlugin() as oJob:  # to enforce to call plugin destructor
            oJob.set_my_name('ga_get_day')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] params or --noauth_local_webserver is required for console execution.')
