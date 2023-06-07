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

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name + '(20230605)')
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

        """ Authenticate and construct service. """
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
        if s_version == 'ga4':  # ga4
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
            # Unique purchases divided by views of product detail pages (Enhanced Ecommerce)
            lst_to_query.append({'met': ['purchaseToViewRate'],
                                 'dim': ['itemName'],
                                 'filter': ['itemName'],
                                 'filename': 'buyToDetailRate'})
            # Product adds divided by views of product details (Enhanced Ecommerce).
            lst_to_query.append({'met': ['cartToViewRate'],
                                 'dim': ['itemName'],
                                 'filter': ['itemName'],
                                 'filename': 'cartToDetailRate'})
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
        n_retry_backoff_cnt = 0
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
                        lst_filters.append(
                            OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name=s_single_filter), desc=True))

                while self._continue_iteration():  # loop for each report date
                    try:
                        dt_retrieval = list(dict_date.keys())[
                            list(dict_date.values()).index(0)]  # find unhandled report task
                    except ValueError:
                        break

                    s_data_date_mysql = dt_retrieval.strftime('%Y%m%d')
                    self._print_debug('--> ' + s_ga4_property_id + ' retrieves ' + s_ua + '-' + s_filename \
                                      + ' report on ' + s_data_date_mysql)
                    s_tsv_filename = s_data_date_mysql + '_' + s_ua + '_' + s_filename + '.tsv'
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
                    # except HttpError as error:
                    #     # https://developers.google.com/analytics/devguides/reporting/core/v4/errors
                    #     if error.resp.reason in ['internalServerError', 'backendError']:
                    #         if nRetryBackoffCnt < 5:
                    #             self._print_debug('start retrying with exponential back-off that GA recommends.')
                    #             self._print_debug(error.resp)
                    #             time.sleep((2 ** nRetryBackoffCnt) + random.random())
                    #             nRetryBackoffCnt = nRetryBackoffCnt + 1
                    #         else:
                    #             raise Exception('remove')
                    except Exception as e:
                        self._print_debug(e)
                        self._print_debug(
                            'GA api has reported weird error while processing sv account id: ' + s_sv_acct_id)
                        self._print_debug('remove')
                        return
                    del o_request

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

                    if o_response.property_quota:
                        self._print_debug('Tokens per day quota consumed: ' + str(
                            o_response.property_quota.tokens_per_day.consumed) + ', ')
                        self._print_debug('remaining ' + str(o_response.property_quota.tokens_per_day.remaining))
                        self._print_debug('Tokens per hour quota consumed: ' + str(
                            o_response.property_quota.tokens_per_hour.consumed) + ', ')
                        self._print_debug('remaining ' + str(o_response.property_quota.tokens_per_hour.remaining))
                        self._print_debug('Concurrent requests quota consumed: ' + str(
                            o_response.property_quota.concurrent_requests.consumed) + ', ')
                        self._print_debug('remaining ' + str(o_response.property_quota.concurrent_requests.remaining))
                        self._print_debug('Server errors per project per hour quota consumed: ' + str(
                            o_response.property_quota.server_errors_per_project_per_hour.consumed) + ', ')
                        self._print_debug(
                            'remaining ' + str(o_response.property_quota.server_errors_per_project_per_hour.remaining))
                        self._print_debug('Potentially thresholded requests per hour quota consumed: ' + str(
                            o_response.property_quota.potentially_thresholded_requests_per_hour.consumed) + ', ')
                        self._print_debug('remaining ' + str(
                            o_response.property_quota.potentially_thresholded_requests_per_hour.remaining))
                        self._print_debug('remove')
                        return
                        # print(
                        #     f"Tokens per day quota consumed: {o_response.property_quota.tokens_per_day.consumed}, "
                        #     f"remaining: {o_response.property_quota.tokens_per_day.remaining}."
                        # )
                        # print(
                        #     f"Tokens per hour quota consumed: {o_response.property_quota.tokens_per_hour.consumed}, "
                        #     f"remaining: {o_response.property_quota.tokens_per_hour.remaining}."
                        # )
                        # print(
                        #     f"Concurrent requests quota consumed: {o_response.property_quota.concurrent_requests.consumed}, "
                        #     f"remaining: {o_response.property_quota.concurrent_requests.remaining}."
                        # )
                        # print(
                        #     f"Server errors per project per hour quota consumed: {o_response.property_quota.server_errors_per_project_per_hour.consumed}, "
                        #     f"remaining: {o_response.property_quota.server_errors_per_project_per_hour.remaining}."
                        # )
                        # print(
                        #     f"Potentially thresholded requests per hour quota consumed: {o_response.property_quota.potentially_thresholded_requests_per_hour.consumed}, "
                        #     f"remaining: {o_response.property_quota.potentially_thresholded_requests_per_hour.remaining}."
                        # )
                    del o_single_row
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


if __name__ == '__main__':  # for console debugging and execution
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with SvJobPlugin() as oJob:  # to enforce to call plugin destructor
            oJob.set_my_name('ga_get_day')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] params or --noauth_local_webserver is required for console execution.')
