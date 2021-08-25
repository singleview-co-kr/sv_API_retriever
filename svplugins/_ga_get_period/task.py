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
import os, errno

import argparse
import re # https://docs.python.org/3/library/re.html

from apiclient.discovery import build
import httplib2
from oauth2client import file
from oauth2client import client
from oauth2client import tools
from googleapiclient.errors import HttpError
from oauth2client.client import AccessTokenRefreshError

# singleview library
if __name__ == '__main__': # for console debugging
    import sys
    sys.path.append('../../classes')
    import sv_http
    import sv_api_config_parser
    sys.path.append('../../conf') # singleview config
    import basic_config
else: # for platform running
    from classes import sv_http
    from classes import sv_api_config_parser
    # singleview config
    from conf import basic_config # singleview config

class svJobPlugin():
    __g_sVersion = '1.0.0'
    __g_sLastModifiedDate = '4th, Jul 2021'
    __g_oLogger = None
    #__g_sHostName = None
    #__g_sUrl = None
    __g_sConfigLoc = None
    __g_sDataLastDate = None
    __g_sDataFirstDate = None

    def __init__( self, dictParams ):
        """ validate dictParams and allocate params to private global attribute """
        self.__g_oLogger = logging.getLogger(__name__ + ' v'+self.__g_sVersion)
        #self.__g_sUrl = dictParams['host_url']
        self.__g_sConfigLoc = dictParams['config_loc']
        self.__g_sDataLastDate = dictParams['data_last_date'].replace('-','')
        self.__g_sDataFirstDate = dictParams['data_first_date'].replace('-','')

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        pass

    def __printDebug( self, sMsg ):
        if __name__ == '__main__': # for console debugging
            print( sMsg )
        else: # for platform running
            if( self.__g_oLogger is not None ):
                self.__g_oLogger.debug( sMsg )

    def getConsoleAuth(self, argv):
        """ Get console auth with arg  --noauth_local_webserver """
        # to get auth for the first time
        # bring dbs.py gaauth method here
        from googleapiclient import sample_tools

        service, flags = sample_tools.init(
            argv, 'analytics', 'v3', __doc__, __file__,
            scope='https://www.googleapis.com/auth/analytics.readonly')

    def __getHttpResponse(self, sTargetUrl ):
        oSvHttp = sv_http.svHttpCom(sTargetUrl)
        oResp = oSvHttp.getUrl()
        oSvHttp.close()
        if( oResp['error'] == -1 ):
            if( oResp['variables'] ): # oResp['variables'] list has items
                try:
                   oResp['variables']['todo']
                except KeyError: # if ['variables']['todo'] is not defined
                    self.__printDebug( '__checkHttpResp error occured but todo is not defined -> continue')
                else: # if ['variables']['todo'] is defined
                    sTodo = oResp['variables']['todo']
                    if( sTodo == 'stop' ):
                        self.__printDebug('HTTP response raised exception!!')
                        raise Exception(sTodo)
        return oResp

    def procTask(self):
        # oRegEx = re.compile(r"https?:\/\/[\w\-.]+\/\w+\/\w+\w/?$") # host_url pattern ex) /aaa/bbb or /aaa/bbb/
        # m = oRegEx.search(self.__g_sConfigLoc) # match() vs search()
        # if( m ): # if arg matches desinated host_url
        #    sTargetUrl = self.__g_sConfigLoc + '?mode=check_status'
        #    oResp = self.__getHttpResponse( sTargetUrl )
        # else:
        #    oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self.__g_sConfigLoc)
        #    oResp = oSvApiConfigParser.getConfig()
        oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self.__g_sConfigLoc)
        oResp = oSvApiConfigParser.getConfig()

        """ Authenticate and construct service. """
        sClientSecretsJson = basic_config.ABSOLUTE_PATH_BOT + '/conf/google_client_secrets.json'

        # Define the auth scopes to request.
        scope = ['https://www.googleapis.com/auth/analytics.readonly']
        # Authenticate and construct service.
        service = self.__getService('analytics', 'v3', scope, sClientSecretsJson )
        # Try to make a request to the API. Print the results or handle errors.
        
        dict_acct_info = oResp['variables']['acct_info']
        if dict_acct_info is None:
            self.__printDebug('invalid config_loc')
            raise Exception('stop')
        
        s_sv_acct_id = list(dict_acct_info.keys())[0]
        s_acct_title = dict_acct_info[s_sv_acct_id]['account_title']
        s_ga_view_id = dict_acct_info[s_sv_acct_id]['ga_view_id']
        
        #self.__traverseAccountInfo(service)
        try:
            oResult = self.__getInsiteRaw(service, s_sv_acct_id, s_acct_title, s_ga_view_id )
        except TypeError as error:
            # Handle errors in constructing a query.
            self.__printDebug(('There was an error in constructing your query : %s' % error))

        except HttpError as error:
            # Handle API errors.
            self.__printDebug(('Arg, there was an API error : %s : %s' % (error.resp.status, error._get_reason())))

        except AccessTokenRefreshError:
            # Handle Auth errors.
            self.__printDebug ('The credentials have been revoked or expired, please re-run the application to re-authorize')

        """
        try:
            #self.__traverseAccountInfo(service)
            aAcctInfo = oResp['variables']['acct_info']
            if aAcctInfo is not None:
                for sSvAcctId in aAcctInfo:
                    try: 
                        sAcctTitle = aAcctInfo[sSvAcctId]['account_title']
                    except KeyError:
                        sAcctTitle = 'untitled_account'

                    try:
                        sGaViewId = aAcctInfo[sSvAcctId]['ga_view_id']
                    except:
                        raise Exception('remove' )
                    
                    oResult = self.__getInsiteRaw(service, sSvAcctId, sAcctTitle, sGaViewId )
        except TypeError as error:
            # Handle errors in constructing a query.
            self.__printDebug(('There was an error in constructing your query : %s' % error))

        except HttpError as error:
            # Handle API errors.
            self.__printDebug(('Arg, there was an API error : %s : %s' % (error.resp.status, error._get_reason())))

        except AccessTokenRefreshError:
            # Handle Auth errors.
            self.__printDebug ('The credentials have been revoked or expired, please re-run the application to re-authorize')
        """

    def __getInsiteRaw(self, service, sSvAcctId, sAcctTitle, sGaViewId):
        """Executes and returns data from the Core Reporting API.
        This queries the API for the top 25 organic search terms by visits.
        Args:
        service: The service object built by the Google API Python client library.
        sGaViewId: String The profile ID from which to retrieve analytics data.
        Returns:
        The response returned from the Core Reporting API.
        """

    #		sDownloadPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId + '/' + sAcctTitle + '/google_analytics/' + sGaViewId
    #		if( os.path.isdir(sDownloadPath) is False ):
    #			os.makedirs(sDownloadPath)
        
        sDataPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId + '/' + sAcctTitle + '/google_analytics/' + sGaViewId + '/data'
        if( os.path.isdir(sDataPath) is False ):
            os.makedirs(sDataPath)
        sConfPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId + '/' + sAcctTitle + '/google_analytics/' + sGaViewId + '/conf'
        if( os.path.isdir(sConfPath) is False ):
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
        
        # transaction referral retrieval
        #dictTransactionRevenueByTrId = {'met': 'transactionRevenue', 'dim': 'ga:transactionId, ga:sourceMedium, ga:campaign, ga:keyword', 'sort':'ga:sourceMedium', 'filename':'transactionRevenueByTrId'}

        # internal search retrieval
        # dictSearchUniques = {'met': 'searchUniques', 'dim': 'ga:searchKeyword', 'sort':'ga:searchKeyword'}

        # product efficiency retrieval
        # dictListViews = {'met': 'productListViews', 'dim': 'ga:productSku', 'sort':'ga:productSku'}
        # dictListClicks = {'met': 'productListClicks', 'dim': 'ga:productSku', 'sort':'ga:productSku'}
        # dictDetailViews = {'met': 'productDetailViews', 'dim': 'ga:productSku', 'sort':'ga:productSku'}
        # dictBuyToDetailRate = {'met': 'buyToDetailRate', 'dim': 'ga:productSku', 'sort':'ga:productSku'}
        # dictCartToDetailRate = {'met': 'cartToDetailRate', 'dim': 'ga:productSku', 'sort':'ga:productSku'}
        # dictAddsToCart = {'met': 'productAddsToCart', 'dim': 'ga:productSku', 'sort':'ga:productSku'}
        # dictRemovesFromCart = {'met': 'productRemovesFromCart', 'dim': 'ga:productSku', 'sort':'ga:productSku'}
        # dictRevenuePerPurchase = {'met': 'productRevenuePerPurchase', 'dim': 'ga:productSku', 'sort':'ga:productSku'}
        # dictQuantityAddedToCart = {'met': 'quantityAddedToCart', 'dim': 'ga:productSku', 'sort':'ga:productSku'}
        # dictCheckouts = {'met': 'productCheckouts', 'dim': 'ga:productSku', 'sort':'ga:productSku'}
        # dictQuantityCheckedOut = {'met': 'quantityCheckedOut', 'dim': 'ga:productSku', 'sort':'ga:productSku'}
        # dictPurchased = {'met': 'itemQuantity', 'dim': 'ga:productSku', 'sort':'ga:productSku'}
        # lstToQuery = [ dictTransactionRevenueByTrId, dictSearchUniques, dictSearchUniques, dictListViews, dictListClicks, dictDetailViews, dictBuyToDetailRate, dictCartToDetailRate, dictAddsToCart, dictCheckouts, dictRemovesFromCart, dictRevenuePerPurchase, dictQuantityAddedToCart, dictQuantityCheckedOut, dictPurchased ]

        dictInsiteUaSegment = {'PC': 'sessions::condition::ga:deviceCategory==desktop', 'MOB': 'sessions::condition::ga:deviceCategory==mobile,ga:deviceCategory==tablet'}
        isDoneSomething = False
        for sUa in dictInsiteUaSegment:
            for dictSetting in lstToQuery:
                sMet = dictSetting['met']

                try: # set designated download filename if requested, or follow metric name
                    sFileName = dictSetting['filename']
                except KeyError:
                    sFileName = sMet

                try:
                    sEarliestFilepath = sConfPath+'/'+sUa+'_'+sFileName+'.earliest'

                    f = open(sEarliestFilepath, 'r')
                    sMinReportDate = f.readline()
                    dtDateDataRetrieval = datetime.strptime(sMinReportDate, '%Y%m%d') - timedelta(days=1)
                    f.close()
                except FileNotFoundError:
                    dtDateDataRetrieval = datetime.strptime(self.__g_sDataLastDate, '%Y%m%d')

                # if requested date is earlier than first date
                if( dtDateDataRetrieval - datetime.strptime(self.__g_sDataFirstDate, '%Y%m%d') < timedelta(days=0) ): 
                    self.__printDebug( dtDateDataRetrieval )
                    self.__printDebug( datetime.strptime(self.__g_sDataFirstDate, '%Y%m%d') )
                    self.__printDebug('meet first stat date -> remove the job and toggle the job table')
                    raise Exception('completed' )
                    continue;

                sDataDate = dtDateDataRetrieval.strftime('%Y-%m-%d')
                sDataDateForMysql =dtDateDataRetrieval.strftime('%Y%m%d')
                self.__printDebug( '--> '+ sGaViewId +' retrieves ' + sUa + '-' + sFileName + ' report on ' + sDataDateForMysql)

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
                    if( nTotalRst > 0 ):
                        # write data table to file.
                        # with open(sDownloadPath+'/'+sTsvFilename, 'w', encoding='utf-8' ) as out:
                        with open(sDataPath+'/'+sTsvFilename, 'w', encoding='utf-8' ) as out:
                            if oRst.get('rows', []):
                                for row in oRst.get('rows'):
                                    for cell in row:
                                        out.write(cell.replace('"', '').replace("'", '') + '\t')
                                        # out.write(cell + '\t')
                                    out.write( '\n')
                    
                    if( nTotalRst > 0 and nTotalForAllRst > 0 ):
                        isDoneSomething = True
                    
                    time.sleep(2)
                except:
                    self.__printDebug( 'GA api has reported weird error while processing sv account id: ' + sSvAcctId )
                    raise Exception('remove' )
        
        if(isDoneSomething is False):
            self.__printDebug( 'no more report remained -> remove the job and toggle the job table sv account id: ' + sSvAcctId )
            raise Exception('completed' )

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
        storage = file.Storage(basic_config.ABSOLUTE_PATH_BOT + '/conf/google_' + api_name + '.dat')
        credentials = storage.get()
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, storage, flags)

        http = credentials.authorize(http=httplib2.Http())
        # Build the service object.
        service = build(api_name, api_version, http=http)
        return service

if __name__ == '__main__': # for console debugging
	# {'config_loc':'1/test_acct', 'data_last_date':'20180601','data_first_date':'20150726'} or 
	# {'config_loc': 'http://localhost/devel/svtest', 'data_last_date':'20180601','data_first_date':'20150726'}
	dictPluginParams = {'config_loc':'2/test_acct', 'data_last_date':'20200229','data_first_date':'20200229'}
	# dictPluginParams = {'config_loc':'3/test_acct', 'data_last_date':'20200514','data_first_date':'20200401'} 
	with svJobPlugin(dictPluginParams) as oJob: # to enforce to call plugin destructor
		try:
			if( sys.argv[1] ):
				oJob.getConsoleAuth( sys.argv )
		except IndexError as error:
			oJob.procTask()

'''
def __printResults(self,results):
		"""Prints out the results.
		This prints out the profile name, the column headers, and all the rows of data.
		Args:
		results: The response returned from the Core Reporting API.
		"""
		self.__printDebug('Profile Name: %s' % results.get('profileInfo').get('profileName'))

		# Print header.
		output = []
		for header in results.get('columnHeaders'):
			output.append('%30s' % header.get('name'))
		self.__printDebug(''.join(output))

		# Print data table.
		if results.get('rows', []):
			for row in results.get('rows'):
				output = []
				for cell in row:
					output.append('%30s' % cell)
				self.__printDebug(''.join(output))
		else:
			self.__printDebug('No Rows Found')

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
			self.__printDebug('> account ID '+ currentAccountId )
			webproperties = service.management().webproperties().list(
				accountId = currentAccountId).execute()

			for WebpropertyId in webproperties.get('items'):
				CurrentWebpropertyId = WebpropertyId.get('id')
				self.__printDebug('-> property ' + WebpropertyId.get('name') + ' ID '+ CurrentWebpropertyId )

				profiles = service.management().profiles().list(
					accountId=currentAccountId,
					webPropertyId=CurrentWebpropertyId).execute()

				for profile in profiles.get('items'):
					#self.__printDebug( profile )
					self.__printDebug( '--> view ' + profile.get('name') + ' ID ' + profile.get('id') )

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
		self.__printDebug('Profile Name: %s' % results.get('profileInfo').get('profileName'))

		# Print header.
		output = []
		for header in results.get('columnHeaders'):
			output.append('%30s' % header.get('name'))
		self.__printDebug(''.join(output))

		# Print data table.
		if results.get('rows', []):
			for row in results.get('rows'):
				output = []
				for cell in row:
					output.append('%30s' % cell)
				self.__printDebug(''.join(output))
		else:
			self.__printDebug('No Rows Found')
'''