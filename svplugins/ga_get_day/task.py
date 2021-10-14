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
from datetime import datetime, timedelta
import time
import os
import sys
import argparse
import random
import httplib2
from apiclient.errors import HttpError
from apiclient.discovery import build

from oauth2client import file
from oauth2client import client
from oauth2client import tools
from googleapiclient.errors import HttpError
from oauth2client.client import AccessTokenRefreshError

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    import sv_object, sv_plugin
    sys.path.append('../../conf') # singleview config
    import basic_config
    #import googleads_config
else:
    from svcommon import sv_object, sv_plugin
    # singleview config
    from conf import basic_config # singleview config


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        self._g_sVersion = '1.0.1'
        self._g_sLastModifiedDate = '12th, Oct 2021'
        self._g_oLogger = logging.getLogger(__name__ + ' v'+self._g_sVersion)
        
    def getConsoleAuth(self, argv):
        """ Get console auth with arg  --noauth_local_webserver """
        # move this method to ga job plugin?
        self._printDebug( '1. place ./client_secret.json downloaded from google to root directory(where ./dbs.py is located)' )
        self._printDebug( '2. run python dbs.py gaauth' )
        self._printDebug( '3. move newly created analytics.dat to ./conf/google_analytics.dat directory manually' )
        self._printDebug( '4. move client_secret.json to ./conf/google_client_secret.json' )
        """ to get auth for the first time, get console auth with arg  --noauth_local_webserver """
        # to get auth for the first time
        # bring dbs.py gaauth method here
        from googleapiclient import sample_tools

        service, flags = sample_tools.init(
            argv, 'analytics', 'v3', __doc__, __file__,
            scope='https://www.googleapis.com/auth/analytics.readonly')

    def do_task(self, o_callback):
        oResp = self._task_pre_proc(o_callback)
        dict_acct_info = oResp['variables']['acct_info']
        if dict_acct_info is None:
            self._printDebug('stop -> invalid config_loc')
            return

        """ Authenticate and construct service. """
        sClientSecretsJson = os.path.join(basic_config.ABSOLUTE_PATH_BOT, 'conf', 'google_client_secrets.json')

        # Define the auth scopes to request.
        scope = ['https://www.googleapis.com/auth/analytics.readonly']
        # Authenticate and construct service.
        service = self.__getService('analytics', 'v3', scope, sClientSecretsJson )
        # Try to make a request to the API. Print the results or handle errors.
        
        s_sv_acct_id = list(dict_acct_info.keys())[0]
        s_acct_title = dict_acct_info[s_sv_acct_id]['account_title']
        s_ga_view_id = dict_acct_info[s_sv_acct_id]['ga_view_id']
        
        try:
            oResult = self.__getInsiteRaw(service, s_sv_acct_id, s_acct_title, s_ga_view_id )
        except TypeError as error:
            # Handle errors in constructing a query.
            self._printDebug(('There was an error in constructing your query : %s' % error))

        except HttpError as error:
            # Handle API errors.
            self._printDebug(('Arg, there was an API error : %s : %s' % (error.resp.status, error._get_reason())))

        except AccessTokenRefreshError:
            # Handle Auth errors.
            self._printDebug ('The credentials have been revoked or expired, please re-run the application to re-authorize')

        self._task_post_proc(o_callback)

    def __getInsiteRaw(self, service, sSvAcctId, sAcctTitle, sGaViewId):
        """Executes and returns data from the Core Reporting API.
        This queries the API for the top 25 organic search terms by visits.
        Args:
        service: The service object built by the Google API Python client library.
        sGaViewId: String The profile ID from which to retrieve analytics data.
        Returns:
        The response returned from the Core Reporting API.
        """
        sDataPath = os.path.join(basic_config.ABSOLUTE_PATH_BOT, 'files', sSvAcctId, sAcctTitle, 'google_analytics', sGaViewId, 'data')
        if os.path.isdir(sDataPath) is False :
            os.makedirs(sDataPath)
        sConfPath = os.path.join(basic_config.ABSOLUTE_PATH_BOT, 'files', sSvAcctId, sAcctTitle, 'google_analytics', sGaViewId, 'conf')
        if os.path.isdir(sConfPath) is False :
            os.makedirs(sConfPath)
        
        # https://developers.google.com/analytics/devguides/reporting/core/dimsmets
        # https://ga-dev-tools.appspot.com/query-explorer/
        # dictation format : metric, dimension, sort, designated_filename
        dictSession = {'met': 'sessions', 'dim': 'ga:sourceMedium, ga:campaign, ga:keyword', 'sort':'ga:sourceMedium'}
        dictBounceRate = {'met': 'bounceRate', 'dim': 'ga:sourceMedium, ga:campaign, ga:keyword', 'sort':'ga:sourceMedium'}
        dictPercentNewSessions = {'met': 'percentNewSessions', 'dim': 'ga:sourceMedium, ga:campaign, ga:keyword', 'sort':'ga:sourceMedium'}
        dictPageviewsPerSession = {'met': 'pageviewsPerSession', 'dim': 'ga:sourceMedium, ga:campaign, ga:keyword', 'sort':'ga:sourceMedium'}
        dictAvgSessionDuration = {'met': 'avgSessionDuration', 'dim': 'ga:sourceMedium, ga:campaign, ga:keyword', 'sort':'ga:sourceMedium'}
        dictTransactions = {'met': 'transactions', 'dim': 'ga:sourceMedium, ga:campaign, ga:keyword', 'sort':'ga:sourceMedium'} # will be removed
        dictTransactionRevenue = {'met': 'transactionRevenue', 'dim': 'ga:sourceMedium, ga:campaign, ga:keyword', 'sort':'ga:sourceMedium'} # will be removed
        lstToQuery = [ dictSession, dictBounceRate, dictPercentNewSessions, dictPageviewsPerSession, dictAvgSessionDuration, dictTransactions, dictTransactionRevenue ]

        # sToday = time.strftime('%Y%m%d')
        nRetryBackoffCnt = 0
        dictInsiteUaSegment = {'PC': 'sessions::condition::ga:deviceCategory==desktop', 'MOB': 'sessions::condition::ga:deviceCategory==mobile,ga:deviceCategory==tablet'}
        for sUa in dictInsiteUaSegment:
            for dictSetting in lstToQuery:
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
                        self._printDebug( '--> '+ sGaViewId +' retrieves ' + sUa + '-' + sFileName + ' report on ' + sDataDateForMysql)
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
                            with open( os.path.join(sDataPath, sTsvFilename), 'w', encoding='utf-8' ) as out:
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
                            if error.resp.reason in ['userRateLimitExceeded', 'quotaExceeded','internalServerError', 'backendError']:
                                if nRetryBackoffCnt < 5:
                                    self._printDebug( 'start retrying with exponential back-off that GA recommends.' )
                                    self._printDebug( error.resp )
                                    time.sleep((2 ** nRetryBackoffCnt ) + random.random())
                                    nRetryBackoffCnt = nRetryBackoffCnt + 1
                                else:
                                    raise Exception('remove' )
                    except ValueError:
                        break

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
        storage = file.Storage(os.path.join(basic_config.ABSOLUTE_PATH_BOT, 'conf', 'google_' + api_name + '.dat'))
        credentials = storage.get()
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, storage, flags)

        http = credentials.authorize(http=httplib2.Http())
        # Build the service object.
        service = build(api_name, api_version, http=http)
        return service

if __name__ == '__main__': # for console debugging and execution
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        if sys.argv[1] == '--noauth_local_webserver':
            print('noauth')
            with svJobPlugin() as oJob: # to enforce to call plugin destructor
            	oJob.getConsoleAuth( sys.argv )
        else:
            with svJobPlugin() as oJob: # to enforce to call plugin destructor
                oJob.set_my_name('aw_get_day')
                oJob.parse_command(sys.argv)
                oJob.do_task(None)
    else:
        print('warning! [analytical_namespace] [config_loc] params or --noauth_local_webserver is required for console execution.')

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
'''