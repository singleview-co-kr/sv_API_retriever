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
import csv

import re # https://docs.python.org/3/library/re.html

from google.ads.googleads.v6.enums.types.device import DeviceEnum
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
# https://developers.google.com/google-ads/api/fields/v6/segments
# https://developers.google.com/google-ads/api/docs/query/overview
# cd /usr/local/lib/python3.7/site-packages/google/ads/googleads/v6/enums

# singleview library
if __name__ == '__main__': # for console debugging
    import sys
    sys.path.append('../../classes')
    import sv_http
    import sv_api_config_parser
    sys.path.append('../../conf') # singleview config
    import basic_config
    #import googleads_config
else: # for platform running
    from classes import sv_http
    from classes import sv_api_config_parser
    # singleview config
    from conf import basic_config # singleview config
    #from conf import googleads_config

class svJobPlugin():
    __g_sVersion = '1.0.0'
    __g_sLastModifiedDate = '4th, Jul 2021'
    __g_oLogger = None
    #__g_sUrl = None
    __g_sConfigLoc = None

    def __init__( self, dictParams ):
        """ validate dictParams and allocate params to private global attribute """
        self.__g_oLogger = logging.getLogger(__name__ + ' v'+self.__g_sVersion)
        self.__g_sConfigLoc = dictParams['config_loc']

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
        print(self.__g_sConfigLoc)
        oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self.__g_sConfigLoc)
        oResp = oSvApiConfigParser.getConfig()
        
        dict_acct_info = oResp['variables']['acct_info']
        if dict_acct_info is None:
            self.__printDebug('invalid config_loc')
            raise Exception('stop')
        
        s_sv_acct_id = list(dict_acct_info.keys())[0]
        s_acct_title = dict_acct_info[s_sv_acct_id]['account_title']
        lst_google_ads = dict_acct_info[s_sv_acct_id]['adw_cid']
        
        try:
            for s_googleads_cid in lst_google_ads:
                oResult = self.__getAdwordsRaw(s_sv_acct_id, s_acct_title, s_googleads_cid )
        except TypeError as error:
            # Handle errors in constructing a query.
            self.__printDebug(('There was an error in constructing your query : %s' % error))

        """
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
                    
                    #oResult = self.__getAdwordsRaw(sSvAcctId, sAcctTitle, sAdwordsCid )
                    for sGoogleadsCid in lstGoogleads:
                        oResult = self.__getAdwordsRaw(sSvAcctId, sAcctTitle, sGoogleadsCid )
        except TypeError as error:
            # Handle errors in constructing a query.
            self.__printDebug(('There was an error in constructing your query : %s' % error))
        """

    def __getAdwordsRaw(self, sSvAcctId, sAcctTitle, sAdwordsCid):
        sDownloadPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId + '/' + sAcctTitle + '/adwords/' + sAdwordsCid
        if( os.path.isdir(sDownloadPath) is False ):
            os.makedirs(sDownloadPath)
        
        s_google_ads_yaml_path = basic_config.ABSOLUTE_PATH_BOT + '/conf/google-ads.yaml'
        o_googleads_client = GoogleAdsClient.load_from_storage(s_google_ads_yaml_path, version="v6")
        o_googleads_service = o_googleads_client.get_service("GoogleAdsService")
        
        try:		
            sLatestFilepath = sDownloadPath+'/general.latest'
            f = open(sLatestFilepath, 'r')
            sMaxReportDate = f.readline()
            dtStartRetrieval = datetime.strptime(sMaxReportDate, '%Y%m%d') + timedelta(days=1)
            f.close()
        except FileNotFoundError:
            dtStartRetrieval = datetime.now() - timedelta(days=1)
            
        self.__printDebug('start date :'+dtStartRetrieval.strftime('%Y-%m-%d'))

        # requested report date should not be later than today
        dtDateEndRetrieval = datetime.now() - timedelta(days=1) # yesterday
        dtDateDiff = dtDateEndRetrieval - dtStartRetrieval
        
        nNumDays = int(dtDateDiff.days ) + 1
        dictDateQueue = dict()
        for x in range (0, nNumDays):
            dtElement = dtStartRetrieval + timedelta(days = x)
            sDate = dtElement
            dictDateQueue.update({sDate:0})

        if( len(dictDateQueue ) == 0 ):
            return

        # set device dictionary
        dict_googleads_v6_device = {i.value: i.name for i in DeviceEnum.Device}
        s_google_ads_cid = sAdwordsCid.replace('-', '')
        
        # set report header rows
        lst_report_header_1 = ['google_ads_api (v6)']
        lst_report_header_2 = ['Campaign', 'Ad group', 'Keyword / Placement', 'Impressions', 'Clicks', 'Cost', 'Device', 'Conversions', 'Total conv. value', 'Day']

        while True: # loop for each report date
            try:
                dtRetrieval = list(dictDateQueue.keys())[list(dictDateQueue.values()).index(0)] # find unhandled report task
                sDataDate = dtRetrieval.strftime('%Y-%m-%d')
                sDataDateForMysql = dtRetrieval.strftime('%Y%m%d')
                self.__printDebug( '--> '+ sAdwordsCid +' will retrieve general report on ' + sDataDateForMysql)
                try:
                    sTsvFilename = sDataDateForMysql + '_general.tsv'
                    f = open(sLatestFilepath, 'w')
                    f.write(sDataDateForMysql)
                    f.close()

                    # notice! this query does not retrieve OFF campaign
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
                        WHERE campaign.status = 'ENABLED' AND segments.date = """ + sDataDateForMysql

                    # Issues a search request using streaming.
                    o_disp_campaign_resp = o_googleads_service.search_stream(customer_id=s_google_ads_cid, query=s_disp_campaign_query)
                    
                    lst_logs = []
                    for disp_campaign_batch in o_disp_campaign_resp:
                        for o_disp_campaign_row in disp_campaign_batch.results:
                            dict_disp_campaign = {'CampaignName': None, 'AdGroupName': None, 'Criteria': None, 'Impressions': 0, 'Clicks': 0, 'Cost': 0, 
                                                  'Device': None, 'Conversions': 0, 'ConversionValue': 0, 'Date': None}
                            # print(f"Campaign with ID {o_disp_campaign_row.campaign.id}, {o_disp_campaign_row.campaign.name}, {o_disp_campaign_row.metrics.cost_micros} was found.")
                            lst_campaign_code = o_disp_campaign_row.campaign.name.split('_')
                            # print(list_campaign_code)
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
                                    WHERE segments.date = """ + sDataDateForMysql + ' AND ' + \
                                        'campaign.id = ' + str(o_disp_campaign_row.campaign.id)
                                # print(query2)
                                o_txt_campaign_resp = o_googleads_service.search_stream(customer_id=customer_id, query=s_text_campaign_query)
                                for txt_campaign_batch in o_txt_campaign_resp:
                                    for o_txt_campaign_row in txt_campaign_batch.results:
                                        dict_disp_campaign['CampaignName'] = o_disp_campaign_row.campaign.name
                                        dict_disp_campaign['AdGroupName'] = 'n/a'
                                        dict_disp_campaign['Criteria'] = o_txt_campaign_row.ad_group_criterion.keyword.text
                                        dict_disp_campaign['Impressions'] = o_disp_campaign_row.metrics.impressions  # refer to o_disp_campaign_row because [search_term_view] does not provide 
                                        dict_disp_campaign['Clicks'] = o_txt_campaign_row.metrics.clicks
                                        dict_disp_campaign['Cost'] = o_txt_campaign_row.metrics.cost_micros
                                        dict_disp_campaign['Device'] = dict_googleads_v6_device[o_disp_campaign_row.segments.device]  # refer to o_disp_campaign_row because [search_term_view] does not provide 
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
                                dict_disp_campaign['Device'] = dict_googleads_v6_device[o_disp_campaign_row.segments.device]
                                dict_disp_campaign['Conversions'] = o_disp_campaign_row.metrics.all_conversions
                                dict_disp_campaign['ConversionValue'] = o_disp_campaign_row.metrics.all_conversions_value
                                dict_disp_campaign['Date'] = o_disp_campaign_row.segments.date
                                lst_logs.append(dict_disp_campaign)
                    
                    # write data stream to file.
                    with open(sDownloadPath+'/'+sTsvFilename, 'w', encoding='utf-8' ) as out:
                        wr = csv.writer(out, delimiter='\t')
                        wr.writerow(lst_report_header_1)
                        wr.writerow(lst_report_header_2)
                        for dict_rows in lst_logs:
                            wr.writerow(list(dict_rows.values()))
                    
                    dictDateQueue[dtRetrieval] = 1
                    time.sleep(2)
                except:
                    self.__printDebug('unknown exception occured')
                    raise Exception('remove' )
            except ValueError:
                break		
	
if __name__ == '__main__': # for console debugging and execution
    dictPluginParams = {'config_loc':None} # {'config_loc':'1/test_acct'}
    nCliParams = len(sys.argv)
    if( nCliParams > 1 ):
        for i in range(nCliParams):
            if i is 0:
                continue

            sArg = sys.argv[i]
            for sParamName in dictPluginParams:
                nIdx = sArg.find( sParamName + '=' )
                if( nIdx > -1 ):
                    aModeParam = sArg.split('=')
                    dictPluginParams[sParamName] = aModeParam[1]
                
        #print( dictPluginParams )
        with svJobPlugin(dictPluginParams) as oJob: # to enforce to call plugin destructor
            oJob.procTask()
    else:
        print( 'warning! [config_loc] params are required for console execution.' )
        
'''
def getConsoleAuth(self, argv):
	""" Get console auth with arg  --noauth_local_webserver """
	# to get auth for the first time
	# bring dbs.py gaauth method here
	#from googleapiclient import sample_tools

	#service, flags = sample_tools.init(
	#	argv, 'analytics', 'v3', __doc__, __file__,
	#	scope='https://www.googleapis.com/auth/analytics.readonly')
	pass
'''