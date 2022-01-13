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
import httplib2
# import errno
import argparse

# import re # https://docs.python.org/3/library/re.html
from apiclient.discovery import build
from oauth2client import file
from oauth2client import client
from oauth2client import tools
from googleapiclient.errors import HttpError
from oauth2client.client import AccessTokenRefreshError

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    import sv_object
    import sv_plugin
else:
    from svcommon import sv_object
    from svcommon import sv_plugin

class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    
    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        self._g_sLastModifiedDate = '15th, Jan 2022'
        self._g_oLogger = logging.getLogger(__name__ + ' modified at '+self._g_sLastModifiedDate)
        self._g_dictParam.update({'data_first_date':None, 'data_last_date':None})
        # Declaring a dict outside of __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        self.__g_sDataLastDate = None
        self.__g_sDataFirstDate = None
        self.__g_lstAccessLevel = []

    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        self.__g_sDataLastDate = None
        self.__g_sDataFirstDate = None
        self.__g_lstAccessLevel = []

    def getConsoleAuth(self, argv):
        """ Get console auth with arg  --noauth_local_webserver """
        # to get auth for the first time
        # bring dbs.py gaauth method here
        from googleapiclient import sample_tools

        service, flags = sample_tools.init(
            argv, 'analytics', 'v3', __doc__, __file__,
            scope='https://www.googleapis.com/auth/analytics.readonly')

    def do_task(self, o_callback):
        self._g_oCallback = o_callback
        
        if self._g_dictParam['data_first_date'] is None or \
            self._g_dictParam['data_last_date'] is None:
            self._printDebug('you should designate data_first_date and data_last_date')
            self._task_post_proc(self._g_oCallback)
            return
        self.__g_sDataLastDate = self._g_dictParam['data_first_date'].replace('-','')
        self.__g_sDataFirstDate = self._g_dictParam['data_last_date'].replace('-','')

        oResp = self._task_pre_proc(o_callback)
        dict_acct_info = oResp['variables']['acct_info']
        if dict_acct_info is None:
            self._printDebug('stop -> invalid config_loc')
            self._task_post_proc(self._g_oCallback)
            return

        """ Authenticate and construct service. """
        sClientSecretsJson = os.path.join(self._g_sAbsRootPath, 'conf', 'google_client_secrets.json')

        # Define the auth scopes to request.
        scope = ['https://www.googleapis.com/auth/analytics.readonly']
        # Authenticate and construct service.
        service = self.__getService('analytics', 'v3', scope, sClientSecretsJson )
        # Try to make a request to the API. Print the results or handle errors.
        
        s_sv_acct_id = list(dict_acct_info.keys())[0]
        s_acct_title = dict_acct_info[s_sv_acct_id]['account_title']
        s_version = dict_acct_info[s_sv_acct_id]['google_analytics']['s_version']
        s_property_or_view_id = dict_acct_info[s_sv_acct_id]['google_analytics']['s_property_or_view_id']
        self.__g_lstAccessLevel = dict_acct_info[s_sv_acct_id]['google_analytics']['lst_access_level']
        if s_version == 'ua':  # universal analytics
            try:
                self.__getInsiteRaw(service, s_sv_acct_id, s_acct_title, s_property_or_view_id)
            except TypeError as error:
                # Handle errors in constructing a query.
                self._printDebug(('There was an error in constructing your query : %s' % error))
            except HttpError as error:
                # Handle API errors.
                self._printDebug(('Arg, there was an API error : %s : %s' % (error.resp.status, error._get_reason())))
            except AccessTokenRefreshError:
                # Handle Auth errors.
                self._printDebug ('The credentials have been revoked or expired, please re-run the application to re-authorize')
        elif s_version == 'ga4':
            self._printDebug('plugin is developing')
            
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
        sDataPath = os.path.join(self._g_sAbsRootPath, 'files', sSvAcctId, sAcctTitle, 'google_analytics', sGaViewId, 'data')
        if os.path.isdir(sDataPath) == False :
            os.makedirs(sDataPath)
        sConfPath = os.path.join(self._g_sAbsRootPath, 'files', sSvAcctId, sAcctTitle, 'google_analytics', sGaViewId, 'conf')
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
            # lst_to_query.append({'met': 'buyToDetailRate', 'dim': 'ga:productName', 'sort':'ga:productName'})
            # lst_to_query.append({'met': 'cartToDetailRate', 'dim': 'ga:productName', 'sort':'ga:productName'})
            lst_to_query.append({'met': 'productAddsToCart', 'dim': 'ga:productName', 'sort':'ga:productName'})  # Number of tmes the product was added to the shopping cart
            lst_to_query.append({'met': 'quantityAddedToCart', 'dim': 'ga:productName', 'sort':'ga:productName'})  # Number of product units added to the shopping cart
            lst_to_query.append({'met': 'productRemovesFromCart', 'dim': 'ga:productName', 'sort':'ga:productName'})
            lst_to_query.append({'met': 'productRevenuePerPurchase', 'dim': 'ga:productName', 'sort':'ga:productName'})  # quota burden query; Average product revenue per purchase (commonly used with Product Coupon Code) (ga:itemRevenue / ga:uniquePurchases) - (Enhanced Ecommerce). This field is disallowed in segments.
            lst_to_query.append({'met': 'itemQuantity', 'dim': 'ga:productName', 'sort':'ga:productName'})  # Total number of items purchased. For example, if users purchase 2 frisbees and 5 tennis balls, this will be 7.
            lst_to_query.append({'met': 'productCheckouts', 'dim': 'ga:productName', 'sort':'ga:productName'})  # Number of times the product was included in the check-out process (Enhanced Ecommerce).
            lst_to_query.append({'met': 'quantityCheckedOut', 'dim': 'ga:productName', 'sort':'ga:productName'})  # Number of product units included in check out (Enhanced Ecommerce).
            lst_to_query.append({'met': 'transactions', 'dim': 'ga:sourceMedium, ga:campaign, ga:keyword', 'sort':'ga:sourceMedium'})
            lst_to_query.append({'met': 'transactionRevenue', 'dim': 'ga:transactionId, ga:sourceMedium, ga:campaign, ga:keyword', 'sort':'ga:sourceMedium', 'filename':'transactionRevenueByTrId'})
        
        n_retry_backoff_cnt = 0
        dictInsiteUaSegment = {'PC': 'sessions::condition::ga:deviceCategory==desktop', 
            'MOB': 'sessions::condition::ga:deviceCategory==mobile,ga:deviceCategory==tablet'}
        isDoneSomething = False
        for sUa in dictInsiteUaSegment:
            for dictSetting in lst_to_query:
                sMet = dictSetting['met']
                try: # set designated download filename if requested, or follow metric name
                    sFileName = dictSetting['filename']
                except KeyError:
                    sFileName = sMet
                try:
                    sEarliestFilepath = os.path.join(sConfPath, sUa+'_'+sFileName+'.earliest')
                    f = open(sEarliestFilepath, 'r')
                    sMinReportDate = f.readline()
                    dtDateDataRetrieval = datetime.strptime(sMinReportDate, '%Y%m%d') - timedelta(days=1)
                    f.close()
                except FileNotFoundError:
                    dtDateDataRetrieval = datetime.strptime(self.__g_sDataLastDate, '%Y%m%d')

                # if requested date is earlier than first date
                if dtDateDataRetrieval - datetime.strptime(self.__g_sDataFirstDate, '%Y%m%d') < timedelta(days=0):
                    self._printDebug('retrieval date - ' + str(dtDateDataRetrieval) + ' meets first stat date -> remove the job and toggle the job table')
                    self._printDebug('completed')
                    return

                sDataDate = dtDateDataRetrieval.strftime('%Y-%m-%d')
                sDataDateForMysql =dtDateDataRetrieval.strftime('%Y%m%d')
                self._printDebug('--> '+ sGaViewId +' retrieves ' + sUa + '-' + sFileName + ' report on ' + sDataDateForMysql)
                try:
                    oRst = service.data().ga().get(
                            ids='ga:' + sGaViewId,
                            start_date=sDataDate,
                            end_date=sDataDate,
                            dimensions = dictSetting['dim'], #'ga:sourceMedium, ga:campaign, ga:keyword',
                            metrics = 'ga:'+sMet,
                            segment = dictInsiteUaSegment[sUa],
                            sort = '-' + dictSetting['sort'], #'-ga:sourceMedium',
                            #filters='ga:medium==organic',
                            #start_index='1',
                            max_results='2000').execute()
                    
                    sTsvFilename = sDataDateForMysql + '_' + sUa + '_' + sFileName + '.tsv'
                    f = open(sEarliestFilepath, 'w')
                    f.write(sDataDateForMysql)
                    f.close()

                    nTotalRst = int(oRst.get('totalResults'))
                    dictResult = oRst.get('totalsForAllResults')
                    nTotalForAllRst = int(float(dictResult.get('ga:' + sMet )))
                    if nTotalRst > 0:
                        # write data table to file.
                        with open(os.path.join(sDataPath, sTsvFilename), 'w', encoding='utf-8') as out:
                            if oRst.get('rows', []):
                                for row in oRst.get('rows'):
                                    for cell in row:
                                        out.write(cell.replace('"', '').replace("'", '') + '\t')
                                    out.write( '\n')
                    
                    if nTotalRst > 0 and nTotalForAllRst > 0:
                        isDoneSomething = True
                    
                    time.sleep(2)
                except HttpError as error:
                    # https://developers.google.com/analytics/devguides/reporting/core/v4/errors
                    if error.resp.reason in ['quotaExceeded']:
                        self._printDebug('stop - daily or monthly quota exceeded')
                        return
                    elif error.resp.reason in ['userRateLimitExceeded','internalServerError', 'backendError']:
                        if n_retry_backoff_cnt < 5:
                            self._printDebug('start retrying with exponential back-off that GA recommends.')
                            self._printDebug(error.resp)
                            time.sleep((2 ** n_retry_backoff_cnt) + random.random())
                            n_retry_backoff_cnt += 1
                        else:
                            raise Exception('remove')
                except Exception as e:
                    self._printDebug(e)
                    self._printDebug('GA api has reported weird error while processing sv account id: ' + sSvAcctId)
                    self._printDebug('remove')  # raise Exception('remove' )
                    return
        
        if isDoneSomething == False:
            self._printDebug('no more report remained -> remove the job and toggle the job table sv account id: ' + sSvAcctId)
            self._printDebug('completed')  # raise Exception('completed' )
            return

    def __getService(self, api_name, api_version, scope, client_secrets_path):
        """Get a service that communicates to a Google API.
        Args:
        api_name: string The name of the api to connect to.
        api_version: string The api version to connect to.
        scope: A list of strings representing the auth scopes to authorize for the connection.
        client_secrets_path: string A path to a valid client secrets file.

        Returns:
        A service that is connected to the specified API.
        """
        # Parse command-line arguments.
        parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            parents=[tools.argparser])
        flags = parser.parse_args([])

        # Set up a Flow object to be used if we need to authenticate.
        flow = client.flow_from_clientsecrets(
            client_secrets_path, scope=scope,
            message=tools.message_if_missing(client_secrets_path))

        # Prepare credentials, and authorize HTTP object with them.
        # If the credentials don't exist or are invalid run through the native client
        # flow. The Storage object will ensure that if successful the good
        # credentials will get written back to a file.
        # storage = file.Storage(basic_config.ABSOLUTE_PATH_BOT + '/conf/google_' + api_name + '.dat')
        storage = file.Storage(os.path.join(self._g_sAbsRootPath, 'conf', 'google_' + api_name + '.dat'))
        credentials = storage.get()
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, storage, flags)

        http = credentials.authorize(http=httplib2.Http())
        # Build the service object.
        service = build(api_name, api_version, http=http)
        return service

if __name__ == '__main__': # for console debugging and execution
    nCliParams = len(sys.argv)
    if nCliParams > 3:
        if sys.argv[1] == '--noauth_local_webserver':
            print('noauth')
            with svJobPlugin() as oJob: # to enforce to call plugin destructor
            	oJob.getConsoleAuth( sys.argv )
        else:
            # {'config_loc':'1/test_acct', 'data_first_date':'20150726', 'data_last_date':'20180601'}
            dictPluginParams = {'config_loc':'2/test_acct', 'data_first_date':'20200229', 'data_last_date':'20200229'}
            with svJobPlugin() as oJob: # to enforce to call plugin destructor
                oJob.set_my_name('ga_get_day')
                oJob.parse_command(sys.argv)
                oJob.do_task(None)
    else:
        print('warning! [config_loc] params or --noauth_local_webserver is required for console execution.')

'''
def __traverseAccountInfo(self, service):
	"""Traverses Management API to return the all property and view id.
	Args:
		service: The service object built by the Google API Python client library.
	Returns:
		A object with the all property and view ID. None if a user does not have any accounts, webproperties, or profiles.
	"""
	accounts = service.management().accounts().list().execute()
	if accounts.get('items'):
		for account in accounts.get('items'):
		
			currentAccountId = account.get('id')
			self._printDebug('> account ID '+ currentAccountId )
			webproperties = service.management().webproperties().list(
				accountId = currentAccountId).execute()

			for WebpropertyId in webproperties.get('items'):
				CurrentWebpropertyId = WebpropertyId.get('id')
				self._printDebug('-> property ' + WebpropertyId.get('name') + ' ID '+ CurrentWebpropertyId )

				profiles = service.management().profiles().list(
					accountId=currentAccountId,
					webPropertyId=CurrentWebpropertyId).execute()

				for profile in profiles.get('items'):
					#self._printDebug( profile )
					self._printDebug( '--> view ' + profile.get('name') + ' ID ' + profile.get('id') )

def __getFirstProfileId(self, service):
	"""Traverses Management API to return the first profile id.
	This first queries the Accounts collection to get the first account ID.
	This ID is used to query the Webproperties collection to retrieve the first
	webproperty ID. And both account and webproperty IDs are used to query the
	Profile collection to get the first profile id.
	Args:
		service: The service object built by the Google API Python client library.
	Returns:
		A string with the first profile ID. None if a user does not have any accounts, webproperties, or profiles.
	"""
	accounts = service.management().accounts().list().execute()
	
	if accounts.get('items'):
		firstAccountId = accounts.get('items')[0].get('id')
		webproperties = service.management().webproperties().list(
			accountId = firstAccountId).execute()
		
		if webproperties.get('items'):
			firstWebpropertyId = webproperties.get('items')[0].get('id')
			profiles = service.management().profiles().list(
				accountId=firstAccountId,
				webPropertyId=firstWebpropertyId).execute()
			
			if profiles.get('items'):
				return profiles.get('items')[0].get('id')

	return None

def __printResults(self,results):
		"""Prints out the results.
		This prints out the profile name, the column headers, and all the rows of data.
		Args:
		results: The response returned from the Core Reporting API.
		"""
		self._printDebug('Profile Name: %s' % results.get('profileInfo').get('profileName'))

		# Print header.
		output = []
		for header in results.get('columnHeaders'):
			output.append('%30s' % header.get('name'))
		self._printDebug(''.join(output))

		# Print data table.
		if results.get('rows', []):
			for row in results.get('rows'):
				output = []
				for cell in row:
					output.append('%30s' % cell)
				self._printDebug(''.join(output))
		else:
			self._printDebug('No Rows Found')
'''