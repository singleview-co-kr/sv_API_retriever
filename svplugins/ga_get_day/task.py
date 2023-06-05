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
# import httplib2
# import argparse
# from apiclient.errors import HttpError
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


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name+'(20230605)')
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
        # Define the auth scopes to request.
        scope = ['https://www.googleapis.com/auth/analytics.readonly']
        # Authenticate and construct service.
        # sClientSecretsJson = os.path.join(self._g_sAbsRootPath, 'conf', 'client_secret_google_analytics.json')
        # service = self.__get_service_oob('analytics', 'v3', scope, sClientSecretsJson)
        sPrivateKeyJson = os.path.join(self._g_sAbsRootPath, 'conf', 'private_key_google_analytics.json')
        service = self.__get_service_oauth2('analytics', 'v3', scope, sPrivateKeyJson)
        # Try to make a request to the API. Print the results or handle errors.        
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
            try:
                self.__getInsiteRaw(service, s_sv_acct_id, s_brand_id, s_property_or_view_id)
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
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return

        self._task_post_proc(self._g_oCallback)
        
    def __getInsiteRaw(self, service, sSvAcctId, sAcctTitle, sGaViewId):
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
            
        # sToday = time.strftime('%Y%m%d')
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

    # def getConsoleAuth(self, argv):
    #     """ Get console auth with arg  --noauth_local_webserver """
    #     # move this method to ga job plugin?
    #     self._print_debug( '1. place ./client_secret.json downloaded from google to root directory(where ./dbs.py is located)' )
    #     self._print_debug( '2. run python dbs.py gaauth' )
    #     self._print_debug( '3. move newly created analytics.dat to ./conf/google_analytics.dat directory manually' )
    #     self._print_debug( '4. move client_secret.json to ./conf/google_client_secret.json' )
    #     """ to get auth for the first time, get console auth with arg  --noauth_local_webserver """
    #     # to get auth for the first time
    #     # bring dbs.py gaauth method here
    #     from googleapiclient import sample_tools

    #     service, flags = sample_tools.init(
    #         argv, 'analytics', 'v3', __doc__, __file__,
    #         scope='https://www.googleapis.com/auth/analytics.readonly')
    
    # def __get_service_oob(self, api_name, api_version, scope, client_secrets_path):
    #     """Get a service that communicates to a Google API.
    #     Args:
    #     api_name: string The name of the api to connect to.
    #     api_version: string The api version to connect to.
    #     scope: A list of strings representing the auth scopes to authorize for the connection.
    #     client_secrets_path: string A path to a valid client secrets file.

    #     Returns:
    #     A service that is connected to the specified API.
    #     """
    #     # Parse command-line arguments.
    #     parser = argparse.ArgumentParser(
    #         formatter_class=argparse.RawDescriptionHelpFormatter,
    #         parents=[tools.argparser])
    #     flags = parser.parse_args([])

    #     # Set up a Flow object to be used if we need to authenticate.
    #     flow = client.flow_from_clientsecrets(
    #         client_secrets_path, scope=scope,
    #         message=tools.message_if_missing(client_secrets_path))

    #     # Prepare credentials, and authorize HTTP object with them.
    #     # If the credentials don't exist or are invalid run through the native client flow.
    #     # The Storage object will ensure that if successful the good credentials will get written back to a file.
    #     storage = file.Storage(os.path.join(self._g_sAbsRootPath, 'conf', 'google_' + api_name + '.dat'))
    #     credentials = storage.get()
    #     if credentials is None or credentials.invalid:
    #         credentials = tools.run_flow(flow, storage, flags)

    #     http = credentials.authorize(http=httplib2.Http())
    #     # Build the service object.
    #     service = build(api_name, api_version, http=http)
    #     return service


if __name__ == '__main__': # for console debugging and execution
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        # if sys.argv[1] == '--noauth_local_webserver':
        #     print('noauth')
        #     with svJobPlugin() as oJob: # to enforce to call plugin destructor
        #     	oJob.getConsoleAuth( sys.argv )
        # else:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('ga_get_day')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] params or --noauth_local_webserver is required for console execution.')
