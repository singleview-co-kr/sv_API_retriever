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
import re # https://docs.python.org/3/library/re.html

from googleads import adwords
from googleads import oauth2

# singleview library
if __name__ == '__main__': # for console debugging
	import sys
	sys.path.append('../../classes')
	import sv_http
	import sv_api_config_parser
	sys.path.append('../../conf') # singleview config
	import basic_config
	import googleads_config
else: # for platform running
	from classes import sv_http
	from classes import sv_api_config_parser
	# singleview config
	from conf import basic_config # singleview config
	from conf import googleads_config

class svJobPlugin():
	__g_sVersion = '0.0.7'
	__g_sLastModifiedDate = '2nd, Jan 2019'
	__g_oLogger = None
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
		#sTargetUrl = self.__g_sUrl + '?mode=check_status'
		#oResp = self.__getHttpResponse( sTargetUrl )

		oRegEx = re.compile(r"https?:\/\/[\w\-.]+\/\w+\/\w+\w/?$") # host_url pattern ex) /aaa/bbb or /aaa/bbb/
		m = oRegEx.search(self.__g_sConfigLoc) # match() vs search()
		if( m ): # if arg matches desinated host_url
			sTargetUrl = self.__g_sConfigLoc + '?mode=check_status'
			oResp = self.__getHttpResponse( sTargetUrl )
		else:
			oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self.__g_sConfigLoc)
			oResp = oSvApiConfigParser.getConfig()

		try:
			aAcctInfo = oResp['variables']['acct_info']
			if aAcctInfo is not None:
				for sSvAcctId in aAcctInfo:
					try: 
						sAcctTitle = aAcctInfo[sSvAcctId]['account_title']
					except KeyError:
						sAcctTitle = 'untitled_account'

					try:
						#sAdwordsCid = aAcctInfo[sSvAcctId]['adw_cid']
						lstGoogleads = aAcctInfo[sSvAcctId]['adw_cid']
					except:
						raise Exception('remove' )
					
					#oResult = self.__getAdwordsRaw( sSvAcctId, sAcctTitle, sAdwordsCid )
					for sGoogleadsCid in lstGoogleads:
						oResult = self.__getAdwordsRaw(sSvAcctId, sAcctTitle, sGoogleadsCid )
		except TypeError as error:
			# Handle errors in constructing a query.
			self.__printDebug(('There was an error in constructing your query : %s' % error))
	
	def __getAdwordsRaw(self, sSvAcctId, sAcctTitle, sAdwordsCid):
		sDownloadPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId + '/' + sAcctTitle + '/adwords/' + sAdwordsCid
		if( os.path.isdir(sDownloadPath) is False ):
			os.makedirs(sDownloadPath)

		# https://developers.google.com/adwords/api/docs/guides/accounts-overview?hl=ko#test_accounts
		# https://medium.com/@MihaZelnik/how-to-create-test-account-for-adwords-api-503ca72b25a4
		# MCC = My Customer Center
		# https://developers.google.com/adwords/api/docs/
		# https://github.com/googleads/googleads-python-lib
		# https://github.com/googleads/googleads-python-lib/releases
		# https://www.youtube.com/watch?v=80KOeuCNc0c

		sClientId = googleads_config.CLIENT_ID
		sClientSecret = googleads_config.CLIENT_SECRET
		sRefreshToken = googleads_config.REFRESH_TOKEN
		sDeveloperToken = googleads_config.DEVELOPER_TOKEN
		sUserAgent = googleads_config.USER_AGENT
		sClientCustomerId = sAdwordsCid

		oauth2_client = oauth2.GoogleRefreshTokenClient(sClientId, sClientSecret, sRefreshToken)
		adwords_client = adwords.AdWordsClient(sDeveloperToken, oauth2_client, sUserAgent,client_customer_id=sClientCustomerId)
		customer_service = adwords_client.GetService('CustomerService',version='v201809')
		customers = customer_service.getCustomers()
		report_downloader = adwords_client.GetReportDownloader(version='v201809')

		try:
			sEarliestFilepath = sDownloadPath+'/general.earliest'
			f = open(sEarliestFilepath, 'r')
			sMinReportDate = f.readline()
			dtDateDataRetrieval = datetime.strptime(sMinReportDate, '%Y%m%d') - timedelta(days=1)
			f.close()
		except FileNotFoundError:
			dtDateDataRetrieval = datetime.strptime(self.__g_sDataLastDate, '%Y%m%d')
		
		# if requested date is earlier than first date
		if( dtDateDataRetrieval - datetime.strptime(self.__g_sDataFirstDate, '%Y%m%d') < timedelta(days=0) ): 
			self.__printDebug('meet first stat date -> remove the job and toggle the job table')
			raise Exception('completed' )
			return

		sDataDate = dtDateDataRetrieval.strftime('%Y-%m-%d')
		sDataDateForMysql =dtDateDataRetrieval.strftime('%Y%m%d')
		self.__printDebug( '--> '+ sAdwordsCid +' will retrieve general report on ' + sDataDateForMysql)
		
		try:
			sTsvFilename = sDataDateForMysql + '_general.tsv'
			self.__printDebug(sTsvFilename)
			
			f = open(sEarliestFilepath, 'w')
			f.write(sDataDateForMysql)
			f.close()

			sDuringStatement = sDataDateForMysql+','+sDataDateForMysql # example .During('20180628,20180628') or 'YESTERDAY' or 'LAST_7_DAYS'
			report_query = (adwords.ReportQueryBuilder() # https://developers.google.com/adwords/api/docs/appendix/reports/criteria-performance-report
				.Select('CampaignName', 'AdGroupName', 'Criteria', 'Impressions', 'Clicks',  # Criteria means keyword in this context
					  'Cost', 'Device', 'Conversions', 'ConversionValue', 'Date')  # https://developers.google.com/adwords/api/docs/appendix/selectorfields
				.From('CRITERIA_PERFORMANCE_REPORT')
				.Where('Status').In('ENABLED' )
				.During(sDuringStatement)
				.Build())

			# http://googleads.github.io/googleads-python-lib/googleads.adwords.ReportDownloader-class.html#DownloadReportAsStreamWithAwql
			# https://developers.google.com/adwords/api/docs/samples/python/reporting
			strRst = report_downloader.DownloadReportAsStringWithAwql(
				report_query, 'TSV', skip_report_header=False, # https://developers.google.com/adwords/api/docs/guides/reporting-concepts
				skip_column_header=False, skip_report_summary=False, include_zero_impressions=False)

			aRstLines = strRst.split('\n')
			if(len(aRstLines) <= 4 ):
				if( 'Total' in aRstLines[2] ):
					self.__printDebug('nothing')
			else:
				# write data table to file.
				with open(sDownloadPath+'/'+sTsvFilename, 'w', encoding='utf-8' ) as out:
					out.write( strRst )

		except:
			self.__printDebug( 'ADWORDS api has reported weird error while processing sv account id: ' + sSvAcctId )
			raise Exception('remove' )

if __name__ == '__main__': # for console debugging
	dictPluginParams = {'config_loc':'2/test_acct', 'data_last_date':'20180903','data_first_date':'20180406'}
	with svJobPlugin(dictPluginParams) as oJob: # to enforce to call plugin destructor
		oJob.procTask()