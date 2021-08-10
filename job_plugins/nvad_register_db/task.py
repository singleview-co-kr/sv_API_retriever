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

# standard library
import logging
from datetime import datetime, timedelta
import time
import os, errno
import shutil
from collections import OrderedDict
import csv
import re
import fileinput
import codecs
import math

# singleview library
if __name__ == '__main__': # for console debugging
    import sys
    sys.path.append('../../classes')
    import sv_http
    import sv_campaign_parser
    import sv_api_config_parser
    import sv_mysql
    sys.path.append('../../conf') # singleview config
    import basic_config
else: # for platform running
    from classes import sv_http
    from classes import sv_mysql
    from classes import sv_campaign_parser
    from classes import sv_api_config_parser
    # singleview config
    from conf import basic_config # singleview config

class svJobPlugin():
    __g_sVersion = '0.0.35'
    __g_sLastModifiedDate = '4th, Jul 2021'
    __g_oLogger = None
    __g_sUrl = None
    __g_sConfigLoc = None
    __g_sMode = None
    #__g_dictMediaTranslator = {'CPC':'cpc', 'DISP':'display'}
    __g_lstDatadateToCompile = [] # create date list to compile NVAD DB

    def __init__( self, dictParams ):
        """ validate dictParams and allocate params to private global attribute """
        self.__g_oLogger = logging.getLogger(__name__ + ' v'+self.__g_sVersion)
        self.__g_sConfigLoc = dictParams['config_loc']
        self.__g_sUrl = dictParams['host_url']
        try:
            self.__g_sMode = dictParams['mode']
        except KeyError:
            pass

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
        # oRegEx = re.compile(r"https?:\/\/[\w\-.]+\/\w+\/\w+\w/?$") # host_url pattern ex) /aaa/bbb or /aaa/bbb/
        # m = oRegEx.search(self.__g_sConfigLoc) # match() vs search()
        # if( m ): # if arg matches desinated host_url
        #	sTargetUrl = self.__g_sConfigLoc + '?mode=check_status'
        #	oResp = self.__getHttpResponse( sTargetUrl )
        # else:
        #	oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self.__g_sConfigLoc)
        #	oResp = oSvApiConfigParser.getConfig()
        oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self.__g_sConfigLoc)
        oResp = oSvApiConfigParser.getConfig()
        
        dict_acct_info = oResp['variables']['acct_info']
        if dict_acct_info is None:
            self.__printDebug('invalid config_loc')
            raise Exception('stop')
            
        s_sv_acct_id = list(dict_acct_info.keys())[0]
        s_acct_title = dict_acct_info[s_sv_acct_id]['account_title']
        dict_nvr_ad_acct = dict_acct_info[s_sv_acct_id]['nvr_ad_acct']
        
        del dict_nvr_ad_acct['manager_login_id'], dict_nvr_ad_acct['api_key'], dict_nvr_ad_acct['secret_key']
        s_cid = list(dict_nvr_ad_acct.keys())[0]
        
        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql:
            oSvMysql.setTablePrefix(s_acct_title)
            oSvMysql.initialize()

        if( self.__g_sMode == None ):
            # check contract_brs_info.tsv facilitate today's info
            dictBrsInfo = self.__defineNvBrspageCost( s_sv_acct_id, s_acct_title, s_cid, datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d') )
            if dictBrsInfo['detected'] == False: # if [contract id] is "svmanual" then dictBrsInfo[sUa] would be -1 
                self.__printDebug( 'no matched contract_brs_info.tsv' )
                raise Exception('stop')
                return
            
            self.__printDebug( '-> register nvad raw data' )
            self.__copyNvadDataFile( s_sv_acct_id, s_acct_title, s_cid )
            self.__parseNvadDataFile( s_sv_acct_id, s_acct_title, s_cid )
        elif( self.__g_sMode == 'recompile' ):
            with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
                oSvMysql.setTablePrefix(s_acct_title)
                oSvMysql.truncateTable('nvad_assembled_daily_log')
                lstStatDate = oSvMysql.executeQuery('getStatDateList' )

                for dictDate in lstStatDate:
                    sCompileDate = datetime.strptime(str(dictDate['date']), '%Y-%m-%d').strftime('%Y%m%d')
                    self.__g_lstDatadateToCompile.append(int(sCompileDate))

                # retrieve manual BRS info if exists
                lstBrsManualDateRange = self.__retrieveNvBrspageManualInfoPeriod(s_sv_acct_id, s_acct_title, s_cid )
                for nManualDatadate in lstBrsManualDateRange:
                    self.__g_lstDatadateToCompile.append(nManualDatadate)

                self.__g_lstDatadateToCompile = sorted(set(self.__g_lstDatadateToCompile))
        elif( self.__g_sMode == 'clear' ):
            self.__printDebug( '-> clear nvad raw data' )
            with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
                oSvMysql.setTablePrefix(s_acct_title)
                oSvMysql.truncateTable('nvad_assembled_daily_log')
                oSvMysql.truncateTable('nvad_master_ad')
                oSvMysql.truncateTable('nvad_master_ad_extension')
                oSvMysql.truncateTable('nvad_master_ad_grp')
                oSvMysql.truncateTable('nvad_master_ad_grp_budget')
                oSvMysql.truncateTable('nvad_master_bizch')
                oSvMysql.truncateTable('nvad_master_campaign')
                oSvMysql.truncateTable('nvad_master_campaign_budget')
                oSvMysql.truncateTable('nvad_master_keyword')
                oSvMysql.truncateTable('nvad_master_qi')
                oSvMysql.truncateTable('nvad_stat_ad')
                oSvMysql.truncateTable('nvad_stat_ad_conversion')
                oSvMysql.truncateTable('nvad_stat_ad_conversion_detail')
                oSvMysql.truncateTable('nvad_stat_ad_detail')
                oSvMysql.truncateTable('nvad_stat_ad_extension')
                oSvMysql.truncateTable('nvad_stat_ad_extension_conversion')
                oSvMysql.truncateTable('nvad_stat_expkeyword')
                oSvMysql.truncateTable('nvad_stat_naverpay_conversion')
            return
        
        # compile registered stat datafile
        nIdx = 0
        nSentinel = len(self.__g_lstDatadateToCompile)
        for nDate in self.__g_lstDatadateToCompile:
            self.__compileDailyRecord( s_sv_acct_id, s_acct_title, s_cid, str(nDate) )
            self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'assemble stat data & register:', suffix = 'Complete', length = 50)
            nIdx += 1

        return
        """
        aAcctInfo = oResp['variables']['acct_info']
        if aAcctInfo is not None:
            for sSvAcctId in aAcctInfo:
                sAcctTitle = aAcctInfo[sSvAcctId]['account_title']
                aNvrAdAcct = aAcctInfo[sSvAcctId]['nvr_ad_acct']
                
                with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql:
                    oSvMysql.setTablePrefix(sAcctTitle)
                    oSvMysql.initialize()

                for cid in aNvrAdAcct:
                    if( cid == 'api_key' or cid == 'secret_key' or cid == 'manager_login_id'):
                        continue
        
                    if( self.__g_sMode == None ):
                        # check contract_brs_info.tsv facilitate today's info
                        dictBrsInfo = self.__defineNvBrspageCost( sSvAcctId, sAcctTitle, cid, datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d') )
                        if dictBrsInfo['detected'] == False: # if [contract id] is "svmanual" then dictBrsInfo[sUa] would be -1 
                            self.__printDebug( 'no matched contract_brs_info.tsv' )
                            raise Exception('stop')
                            return
                        
                        self.__printDebug( '-> register nvad raw data' )
                        self.__copyNvadDataFile( sSvAcctId, sAcctTitle, cid )
                        self.__parseNvadDataFile( sSvAcctId, sAcctTitle, cid )
                    elif( self.__g_sMode == 'recompile' ):
                        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
                            oSvMysql.setTablePrefix(sAcctTitle)
                            oSvMysql.truncateTable('nvad_assembled_daily_log')
                            lstStatDate = oSvMysql.executeQuery('getStatDateList' )

                            for dictDate in lstStatDate:
                                sCompileDate = datetime.strptime(str(dictDate['date']), '%Y-%m-%d').strftime('%Y%m%d')
                                self.__g_lstDatadateToCompile.append(int(sCompileDate))

                            # retrieve manual BRS info if exists
                            lstBrsManualDateRange = self.__retrieveNvBrspageManualInfoPeriod(sSvAcctId, sAcctTitle, cid )
                            for nManualDatadate in lstBrsManualDateRange:
                                self.__g_lstDatadateToCompile.append(nManualDatadate)

                            self.__g_lstDatadateToCompile = sorted(set(self.__g_lstDatadateToCompile))
                    elif( self.__g_sMode == 'clear' ):
                        self.__printDebug( '-> clear nvad raw data' )
                        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
                            oSvMysql.setTablePrefix(sAcctTitle)
                            oSvMysql.truncateTable('nvad_assembled_daily_log')
                            oSvMysql.truncateTable('nvad_master_ad')
                            oSvMysql.truncateTable('nvad_master_ad_extension')
                            oSvMysql.truncateTable('nvad_master_ad_grp')
                            oSvMysql.truncateTable('nvad_master_ad_grp_budget')
                            oSvMysql.truncateTable('nvad_master_bizch')
                            oSvMysql.truncateTable('nvad_master_campaign')
                            oSvMysql.truncateTable('nvad_master_campaign_budget')
                            oSvMysql.truncateTable('nvad_master_keyword')
                            oSvMysql.truncateTable('nvad_master_qi')
                            oSvMysql.truncateTable('nvad_stat_ad')
                            oSvMysql.truncateTable('nvad_stat_ad_conversion')
                            oSvMysql.truncateTable('nvad_stat_ad_conversion_detail')
                            oSvMysql.truncateTable('nvad_stat_ad_detail')
                            oSvMysql.truncateTable('nvad_stat_ad_extension')
                            oSvMysql.truncateTable('nvad_stat_ad_extension_conversion')
                            oSvMysql.truncateTable('nvad_stat_expkeyword')
                            oSvMysql.truncateTable('nvad_stat_naverpay_conversion')
                        return
                    
                    # compile registered stat datafile
                    nIdx = 0
                    nSentinel = len(self.__g_lstDatadateToCompile)
                    for nDate in self.__g_lstDatadateToCompile:
                        self.__compileDailyRecord( sSvAcctId, sAcctTitle, cid, str(nDate) )
                        self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'assemble stat data & register:', suffix = 'Complete', length = 50)
                        nIdx += 1
        
        return
        """

    def __compileDailyRecord(self, sSvAcctId, sAcctTitle, cid, sCompileDate ):
        try: # validate requsted date
            sCompileDate = datetime.strptime(sCompileDate, '%Y%m%d').strftime('%Y-%m-%d')
        except ValueError:
            self.__printDebug( sCompileDate + ' is invalid date string' )
            return

        dictNvBrsPageImpByUa = { 'M':0, 'P':0 }
        oSvCampaignParser = sv_campaign_parser.svCampaignParser()
        
        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            
            # retrieve daily nvad media log ignore weird cid
            lstDailyMediaLog = oSvMysql.executeQuery('getDailyMediaLogs', sCompileDate, cid )

            dictCampaignInfo = {}
            dictAdGrpInfo = {}
            dictKwInfo = {}
            dictCompliedDailyLog = {}
            
            # compile daily nvad log by campaign_id & ad_group_id & ad_keyword_id & ua
            for lstMediLog in lstDailyMediaLog:
                sCampaignId = lstMediLog['campaign_id']+'|@|'+lstMediLog['ad_group_id']+'|@|'+lstMediLog['ad_keyword_id']+'|@|'+lstMediLog['pc_mobile_type'] 
                try: # if designated log already created
                    dictCompliedDailyLog[sCampaignId]
                    dictCompliedDailyLog[sCampaignId]['cost'] += int(lstMediLog['cost'])
                    dictCompliedDailyLog[sCampaignId]['imp'] += int(lstMediLog['impression'])
                    dictCompliedDailyLog[sCampaignId]['click'] += int(lstMediLog['click'])
                except KeyError: # if new log requested
                    try: # if campaign id already retrieved
                        sTranslatedCampaignTitle = dictCampaignInfo[lstMediLog['campaign_id']]
                    except KeyError: # if new campaign id requested
                        lstCampaign = oSvMysql.executeQuery('getCampaignInfo', lstMediLog['campaign_id'], sCompileDate )
                        try:
                            dictCampaignInfo[lstMediLog['campaign_id']] = lstCampaign[0]['campaign_name']
                            sTranslatedCampaignTitle = lstCampaign[0]['campaign_name']
                        except IndexError: # if campaign was existed but permanently removed or unidentified 
                            self.__printDebug( '-1-' )
                            self.__printDebug( lstCampaign )
                            self.__printDebug( sCompileDate )
                            self.__printDebug( lstMediLog['campaign_id'] )
                            dictCampaignInfo[lstMediLog['campaign_id']] = lstMediLog['campaign_id']
                            sTranslatedCampaignTitle = lstMediLog['campaign_id']

                    try: # if ad group id already retrieved
                        sTranslatedGrpTitle = dictAdGrpInfo[lstMediLog['ad_group_id']]
                    except KeyError: # if new ad group id requested
                        lstAdGrp = oSvMysql.executeQuery('getAdGrpInfo', lstMediLog['ad_group_id'], sCompileDate )
                        try:
                            dictAdGrpInfo[lstMediLog['ad_group_id']] = lstAdGrp[0]['ad_group_name']
                            sTranslatedGrpTitle = lstAdGrp[0]['ad_group_name']
                        except IndexError: # if ad grp was existed but permanently removed or unidentified 
                            self.__printDebug( '-2-' )
                            self.__printDebug( lstAdGrp )
                            self.__printDebug( sCompileDate )
                            self.__printDebug( lstMediLog['ad_group_id'] )
                            dictAdGrpInfo[lstMediLog['ad_group_id']] = lstMediLog['ad_group_id']
                            sTranslatedGrpTitle = lstMediLog['ad_group_id']

                    if( lstMediLog['ad_keyword_id'] != '-' ): # ignore non keyword media eg) nvr shopping
                        try: # if ad keyword id already retrieved
                            sTranslatedKw = dictKwInfo[lstMediLog['ad_keyword_id']]
                        except KeyError: # if new ad keyword id requested
                            lstKw = oSvMysql.executeQuery('getKwInfo', lstMediLog['ad_keyword_id'], sCompileDate )
                            try:
                                dictKwInfo[lstMediLog['ad_keyword_id']] = lstKw[0]['ad_keyword']
                                sTranslatedKw = lstKw[0]['ad_keyword']
                            except IndexError: # if ad keyword was existed but permanently removed or unidentified 
                                # this loss is mainly caused by deprecated NVR_brand_search_page removal
                                dictKwInfo[lstMediLog['ad_keyword_id']] = lstMediLog['ad_keyword_id']
                                sTranslatedKw = lstMediLog['ad_keyword_id']
                    else:
                        dictKwInfo[lstMediLog['ad_keyword_id']] = '-'
                        sTranslatedKw = '-'

                    dictCompliedDailyLog[sCampaignId] = {
                        'campaign_id':lstMediLog['campaign_id'],'campaign_name':sTranslatedCampaignTitle,
                        'group_id':lstMediLog['ad_group_id'],'grp_name':sTranslatedGrpTitle,
                        'kw_id':lstMediLog['ad_keyword_id'],'term':sTranslatedKw,
                        'source':'','rst_type':'','media':'','brd':0,'campaign_1st':'','campaign_2nd':'','campaign_3rd':'',
                        'cost':int(lstMediLog['cost']),'imp':int(lstMediLog['impression']),'click':int(lstMediLog['click']),
                        'conv_cnt':0, 'conv_amnt':0,
                        'ua':lstMediLog['pc_mobile_type']
                    }
            
            # get BRS page cost on this date 
            dictBrspageDailyCostRst = self.__defineNvBrspageCost( sSvAcctId, sAcctTitle, cid, sCompileDate )

            # retrieve daily nvad conversion log
            lstDailyConvLogs = oSvMysql.executeQuery('getDailyConversionLogs', sCompileDate, cid )
            
            for lstSingleConvlog in lstDailyConvLogs:
                sConvId = lstSingleConvlog['campaign_id']+'|@|'+lstSingleConvlog['ad_group_id']+'|@|'+lstSingleConvlog['ad_keyword_id']+'|@|'+lstSingleConvlog['pc_mobile_type'] 
                try:
                    dictCompliedDailyLog[sConvId]['conv_cnt'] += int(lstSingleConvlog['conversion_count'])
                    dictCompliedDailyLog[sConvId]['conv_amnt'] += int(lstSingleConvlog['sales_by_conversion'])
                except KeyError: # it is possible to exist conversion log without same day media impression data, in a word, supposed to be a delayed conversion
                    
                    lstCampaign = oSvMysql.executeQuery('getCampaignInfo', lstSingleConvlog['campaign_id'], sCompileDate )
                    try:
                        sTranslatedCampaignTitle = lstCampaign[0]['campaign_name']
                    except IndexError: # if campaign was existed but permanently removed or unidentified 
                        self.__printDebug( '-4-' )
                        self.__printDebug( lstCampaign )
                        self.__printDebug( sCompileDate )
                        self.__printDebug( lstSingleConvlog['campaign_id'] )
                        sTranslatedCampaignTitle = lstSingleConvlog['campaign_id']
                    
                    lstAdGrp = oSvMysql.executeQuery('getAdGrpInfo', lstSingleConvlog['ad_group_id'], sCompileDate )
                    try:
                        sTranslatedGrpTitle = lstAdGrp[0]['ad_group_name']
                    except IndexError: # if ad grp was existed but permanently removed or unidentified 
                        self.__printDebug( '-5-' )
                        self.__printDebug( lstAdGrp )
                        self.__printDebug( sCompileDate )
                        self.__printDebug( lstSingleConvlog['ad_group_id'] )
                        sTranslatedGrpTitle = lstSingleConvlog['ad_group_id']

                    if( lstSingleConvlog['ad_keyword_id'] != '-' ): # ignore non keyword media eg) nvr shopping
                        lstKw = oSvMysql.executeQuery('getKwInfo', lstSingleConvlog['ad_keyword_id'], sCompileDate )
                        try:
                            sTranslatedKw = lstKw[0]['ad_keyword']
                        except IndexError: # if ad keyword was existed but permanently removed or unidentified 
                            self.__printDebug( '-6-' )
                            self.__printDebug( lstKw )
                            self.__printDebug( sCompileDate )
                            self.__printDebug( lstSingleConvlog['ad_keyword_id'] )
                            sTranslatedKw = lstSingleConvlog['ad_keyword_id']
                    else:
                        sTranslatedKw = '-'
                    
                    dictCompliedDailyLog[sCampaignId] = {
                        'campaign_id':lstSingleConvlog['campaign_id'],'campaign_name':sTranslatedCampaignTitle,
                        'group_id':lstSingleConvlog['ad_group_id'],'grp_name':sTranslatedGrpTitle,
                        'kw_id':lstSingleConvlog['ad_keyword_id'],'term':sTranslatedKw,
                        'source':'','rst_type':'','media':'','brd':0,'campaign_1st':'','campaign_2nd':'','campaign_3rd':'',
                        'cost':0,'imp':0,'click':0,
                        'conv_cnt':int(lstSingleConvlog['conversion_count']), 'conv_amnt':int(lstSingleConvlog['sales_by_conversion']),
                        'ua':lstSingleConvlog['pc_mobile_type']
                    }

            # translate compiled log
            for sCampaignId in dictCompliedDailyLog:
                dictCampaignInfo = self.__parseCampaignCode(oSvMysql, dictCompliedDailyLog[sCampaignId], sCompileDate )
                
                dictCompliedDailyLog[sCampaignId]['source'] = dictCampaignInfo['source']
                dictCompliedDailyLog[sCampaignId]['rst_type'] = dictCampaignInfo['rst_type']
                dictCompliedDailyLog[sCampaignId]['media'] = dictCampaignInfo['media']
                dictCompliedDailyLog[sCampaignId]['brd'] = dictCampaignInfo['brd']
                dictCompliedDailyLog[sCampaignId]['campaign_1st'] = dictCampaignInfo['campaign_1st']
                dictCompliedDailyLog[sCampaignId]['campaign_2nd'] = dictCampaignInfo['campaign_2nd']
                dictCompliedDailyLog[sCampaignId]['campaign_3rd'] = dictCampaignInfo['campaign_3rd']
                
                if( dictCampaignInfo['campaign_1st'] == 'BRS' ): # if BRS exists, sum BRS impression total
                    dictNvBrsPageImpByUa[ dictCompliedDailyLog[sCampaignId]['ua'] ] = dictNvBrsPageImpByUa[ dictCompliedDailyLog[sCampaignId]['ua'] ] + dictCompliedDailyLog[sCampaignId]['imp']

                if( dictCompliedDailyLog[sCampaignId]['media'] == 'CPC' and dictCompliedDailyLog[sCampaignId]['campaign_1st'].find('NVSHOP') > -1 ):
                    dictCompliedDailyLog[sCampaignId]['campaign_1st'] = 'NVSHOP'
                    dictCompliedDailyLog[sCampaignId]['term'] = 'nvshop'

            sLogDate = datetime.strptime( sCompileDate, "%Y-%m-%d" )
            
            bBrsInfoFromApiExist = False # API brs info is primary always!
            # insert into DB
            for sCampaignId in dictCompliedDailyLog:
                if( dictCompliedDailyLog[sCampaignId]['campaign_1st'] == 'BRS' ): # if BRS exists, allocate cost based on impression
                    bBrsInfoFromApiExist = True
                    if( dictBrspageDailyCostRst[ dictCompliedDailyLog[sCampaignId]['ua'] ] == 0 ): # raise exception if BRS impression exists without contract info
                        self.__printDebug( 'check brs info: BRS impression exists without contract info on ' + sCompileDate )
                        nCost = 0
                    else:
                        try:
                            nCost = int(dictCompliedDailyLog[sCampaignId]['imp']/dictNvBrsPageImpByUa[ dictCompliedDailyLog[sCampaignId]['ua'] ] * dictBrspageDailyCostRst[ dictCompliedDailyLog[sCampaignId]['ua'] ])
                        except ZeroDivisionError: # brs info with cost exists mistakenly
                            nCost = 0
                            self.__printDebug( 'plz check brs info: errornous situation has been detected:')
                            self.__printDebug( 'brs impression:' + str( dictCompliedDailyLog[sCampaignId]['imp'] ))
                            self.__printDebug( 'brs impression by UA:' + str( dictNvBrsPageImpByUa[ dictCompliedDailyLog[sCampaignId]['ua'] ] ) )
                            self.__printDebug( 'brs daily cost:' + str( dictBrspageDailyCostRst[ dictCompliedDailyLog[sCampaignId]['ua'] ] ) )
                else:
                    nCost = dictCompliedDailyLog[sCampaignId]['cost']

                oSvMysql.executeQuery('insertAssembledNvadLog', 
                    cid, dictCompliedDailyLog[sCampaignId]['campaign_id'], dictCompliedDailyLog[sCampaignId]['campaign_name'], dictCompliedDailyLog[sCampaignId]['group_id'],
                    dictCompliedDailyLog[sCampaignId]['ua'], dictCompliedDailyLog[sCampaignId]['kw_id'], dictCompliedDailyLog[sCampaignId]['term'],
                    dictCompliedDailyLog[sCampaignId]['rst_type'], #self.__g_dictMediaTranslator[dictCompliedDailyLog[sCampaignId]['media']],
                    oSvCampaignParser.getSvMediumTag( dictCompliedDailyLog[sCampaignId]['media'] ),
                    dictCompliedDailyLog[sCampaignId]['brd'], dictCompliedDailyLog[sCampaignId]['campaign_1st'], dictCompliedDailyLog[sCampaignId]['campaign_2nd'],
                    dictCompliedDailyLog[sCampaignId]['campaign_3rd'], nCost,
                    dictCompliedDailyLog[sCampaignId]['imp'], dictCompliedDailyLog[sCampaignId]['click'], dictCompliedDailyLog[sCampaignId]['conv_cnt'],
                    dictCompliedDailyLog[sCampaignId]['conv_amnt'], sLogDate )
            
            # register manual BRS info if allowed
            if bBrsInfoFromApiExist == False: # API brs info is primary always!
                #self.__printDebug( ' manual brs info on ' + sCompileDate )
                lstNvBrsManualInfoByDate = self.__retrieveNvBrspageManualInfoByDate( sSvAcctId, sAcctTitle, cid, sCompileDate )
                for dictNvBrsManualInfo in lstNvBrsManualInfoByDate:
                    #self.__printDebug( dictNvBrsManualInfo )
                    sCampaignId = sCampaignName = sGrpId = sKwId = 'svmanual'
                    sRstType = 'PS'
                    sMedia = 'display'
                    sBrd = '1'
                    sCamp1st = 'BRS'
                    if dictNvBrsManualInfo['ua'] == 'M':
                        sCamp2nd = 'MOB'
                    elif dictNvBrsManualInfo['ua'] == 'P':
                        sCamp2nd = 'PC'
                    sCamp3rd = '00'
                    nCost = 0
                    oSvMysql.executeQuery('insertAssembledNvadLog', 
                        cid, sCampaignId, sCampaignName, sGrpId,
                        dictNvBrsManualInfo['ua'], sKwId, dictNvBrsManualInfo['term'],
                        sRstType, sMedia,
                        sBrd, sCamp1st, sCamp2nd, sCamp3rd, nCost,
                        dictNvBrsManualInfo['imp'], dictNvBrsManualInfo['click'], dictNvBrsManualInfo['conv_cnt'], dictNvBrsManualInfo['conv_amnt'], sLogDate )

    def __defineNvBrspageCost(self, sSvAcctId, sAcctTitle, cid, sCompileDate ):
        dictRst = { 'M':0, 'P':0, 'detected':False }
        try:
            sBrspageInfoFilePath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId +'/' + sAcctTitle + '/naver_ad/' + cid + '/contract_brs_info.tsv'
        except KeyError:
            self.__printDebug( 'invalid brs info' )
            raise Exception('stop')
        
        dtTouchingDate = datetime.strptime(sCompileDate, '%Y-%m-%d').date()

        try:
            with codecs.open(sBrspageInfoFilePath, 'r',encoding='utf8') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                nRowCnt = 0
                for row in reader:
                    if( nRowCnt > 0 ): 
                        if( row[7] != '-' ):
                            aContractPeriod = row[7].split('~')
                            if( len(aContractPeriod[0]) > 0 ): # contract date - start
                                try: # validate requsted date
                                    dtBeginDate = datetime.strptime(aContractPeriod[0], '%Y.%m.%d.').date()
                                except ValueError:
                                    self.__printDebug( 'start date:' + aContractPeriod[0] + ' is invalid date string' )

                            if( len(aContractPeriod[1]) > 0 ): # contract date - end
                                try: # validate requsted date
                                    dtEndDate = datetime.strptime(aContractPeriod[1], '%Y.%m.%d.').date()
                                except ValueError:
                                    self.__printDebug( 'end date:' + aContractPeriod[1] + ' is invalid date string' )

                            if( dtBeginDate <= dtTouchingDate and dtEndDate >= dtTouchingDate ):
                                dictRst['detected'] = True  # tag brs info detected even if cost is 0
                                if row[0] != 'svmanual':
                                    dtDeltaDays = dtEndDate - dtBeginDate
                                    if row[9] == '-':
                                        nRefundAmnt = 0
                                    else:
                                        nRefundAmnt = int(row[9].replace(',', ''))
                                    nNetPeriodCost = int(row[8].replace(',', '')) - nRefundAmnt
                                    nPeriodCostExcVat = math.ceil(nNetPeriodCost / 1.1)
                                    nDailyCost = nPeriodCostExcVat / ( dtDeltaDays.days + 1 )
                                    sUa = row[10] # contract UA
                                    dictRst[row[10]] = dictRst[sUa] + nDailyCost
                                elif row[0] == 'svmanual':
                                    dictRst[row[10]] = -1
                        
                    nRowCnt = nRowCnt + 1
        except FileNotFoundError:
            self.__printDebug( 'pass ' + sBrspageInfoFilePath + ' does not exist')
            raise Exception('stop')
        
        return dictRst

    def __parseCampaignCode(self, oSvMysql, dictCompliedDailyLog, sCompileDate):
        sCampaignCode = dictCompliedDailyLog['grp_name']
        aCampaignDefinition = sCampaignCode.split('_')
        if( len( aCampaignDefinition ) > 5 ):
            sSourceTitle = aCampaignDefinition[0]
            if( sSourceTitle == 'NV' ):
                dictCampaignInfo = {'source':'','rst_type':'','media':'','brd':0,'campaign_1st':'','campaign_2nd':'','campaign_3rd':''}
                dictCampaignInfo['source'] = sSourceTitle
                dictCampaignInfo['rst_type'] = aCampaignDefinition[1]
                dictCampaignInfo['media'] = aCampaignDefinition[2]
                
                if( aCampaignDefinition[2] == 'DISP' and aCampaignDefinition[3] == 'BRS' ):
                    dictCampaignInfo['brd'] = 1
                
                if( aCampaignDefinition[2] == 'CPC' and aCampaignDefinition[3] == 'NVSHOPPING' ):
                    dictCampaignInfo['term'] = 'nvshopping'

                if( aCampaignDefinition[3] == 'BR' ):
                    dictCampaignInfo['brd'] = 1

                dictCampaignInfo['campaign_1st'] = aCampaignDefinition[3]
                dictCampaignInfo['campaign_2nd'] = aCampaignDefinition[4]
                dictCampaignInfo['campaign_3rd'] = aCampaignDefinition[5]
            elif( sSourceTitle == 'GG' ):
                self.__printDebug(aCampaignDefinition)
            elif( sSourceTitle == 'FB' ):
                self.__printDebug(aCampaignDefinition)
            else:
                dictCampaignInfo = {}
        else:
            lstCampaign = oSvMysql.executeQuery('getCampaignInfo', dictCompliedDailyLog['campaign_id'], sCompileDate )

            #if( len( lstCampaign ) == 1 ):
            nCampaignType = lstCampaign[0]['campaign_type']
            if( nCampaignType == 1 ):
                dictCampaignInfo = {'source':'NV','rst_type':'PS','media':'CPC','brd':0,'campaign_1st':dictCompliedDailyLog['campaign_name'], \
                    'campaign_2nd':'','campaign_3rd':''}
            elif( nCampaignType == 2 ):
                dictCampaignInfo = {'source':'NV','rst_type':'PS','media':'CPC','brd':0,'campaign_1st':'NVSHOP', \
                    'campaign_2nd':'', 'campaign_3rd':'', 'term':'nvshop' }
            elif( nCampaignType == 4 ):
                dictCampaignInfo = {'source':'NV','rst_type':'PS','media':'DISP','brd':1,'campaign_1st':'BRS', \
                    'campaign_2nd':dictCompliedDailyLog['grp_name'],'campaign_3rd':''}
            else:
                self.__printDebug( 'werid campaign info' )
                dictCampaignInfo = {}
        
        return dictCampaignInfo

    def __parseNvadDataFile(self, sSvAcctId, sAcctTitle, cid ):
        self.__printDebug( '-> '+ cid +' is registering NVAD data files' )
        sDataPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId +'/' + sAcctTitle + '/naver_ad/' + cid
        
        # dictionary for master data file
        dictBizCh = dict()
        dictCamp = dict()
        dictCampBudget = dict()
        dictAdgrp = dict()
        dictAdgrpBudget = dict()
        dictKw = dict()
        dictMasterAd = dict()
        dictMasterAdExt = dict()
        dictQi = dict()

        # dictionary for stat data file
        dictStatAd = dict()
        dictAdDetail = dict()
        dictAdConversion = dict()
        dictAdConversionDetail = dict()
        dictAdExtension = dict()
        dictAdExtensionConversion = dict()
        dictNpayConversion = dict()
        dictExpkeyword = dict()
        
        # traverse directory and categorize data files
        lstDataFiles = os.listdir(sDataPath)
        for sFilename in lstDataFiles:
            #sDataFileFullname = os.path.join(sDataPath, sFilename)
            
            aFileExt = os.path.splitext(sFilename)
            if( aFileExt[0] == 'agency_info' or aFileExt[0] == 'contract_brs_info' or aFileExt[0] == 'contract_pns_info' or 
                aFileExt[0] == 'alias_adgrp_info' or aFileExt[0] == 'performance_brs_info' ):
                continue
            if( aFileExt[1] == '' or aFileExt[1] == '.latest' or aFileExt[1] == '.earliest' ): # pass if extension is .earliest or .latest or directory
                continue
            
            aFile = sFilename.split('_')
            
            # check a data file type
            if( len( aFile[0] ) == 14 ):
                sDatadate = aFile[0][0:8]
            else:
                sDatadate = aFile[0]
            
            nDatadate = int(sDatadate)
            
            nEndIdx = len(aFile ) #- 1
            sReportType = ''
            for nIdx in range(1, nEndIdx):
                sReportType += aFile[nIdx]
                if nIdx < nEndIdx - 1: 
                    sReportType += '_'

            if sReportType == 'AD.tsv':
                dictStatAd.update({nDatadate:sFilename})
                self.__g_lstDatadateToCompile.append(nDatadate)
            elif sReportType == 'AD_DETAIL.tsv':
                dictAdDetail.update({nDatadate:sFilename})
                self.__g_lstDatadateToCompile.append(nDatadate)
            elif sReportType == 'AD_CONVERSION.tsv':
                dictAdConversion.update({nDatadate:sFilename})
                self.__g_lstDatadateToCompile.append(nDatadate)
            elif sReportType == 'AD_CONVERSION_DETAIL.tsv':
                dictAdConversionDetail.update({nDatadate:sFilename})
                self.__g_lstDatadateToCompile.append(nDatadate)
            elif sReportType == 'ADEXTENSION.tsv':
                dictAdExtension.update({nDatadate:sFilename})
                self.__g_lstDatadateToCompile.append(nDatadate)
            elif sReportType == 'ADEXTENSION_CONVERSION.tsv':
                dictAdExtensionConversion.update({nDatadate:sFilename})
                self.__g_lstDatadateToCompile.append(nDatadate)
            elif sReportType == 'NAVERPAY_CONVERSION.tsv':
                dictNpayConversion.update({nDatadate:sFilename})
                self.__g_lstDatadateToCompile.append(nDatadate)
            elif sReportType == 'EXPKEYWORD.tsv':
                dictExpkeyword.update({nDatadate:sFilename})
                self.__g_lstDatadateToCompile.append(nDatadate)
            elif sReportType == 'BusinessChannel_full.tsv' or sReportType == 'BusinessChannel_delta.tsv':
                dictBizCh.update({nDatadate:sFilename})
            elif sReportType == 'Campaign_full.tsv' or sReportType == 'Campaign_delta.tsv':
                dictCamp.update({nDatadate:sFilename})
            elif sReportType == 'CampaignBudget_full.tsv' or sReportType == 'CampaignBudget_delta.tsv':
                dictCampBudget.update({nDatadate:sFilename})
            elif sReportType == 'Adgroup_full.tsv' or sReportType == 'Adgroup_delta.tsv':
                dictAdgrp.update({nDatadate:sFilename})
            elif sReportType == 'AdgroupBudget_full.tsv' or sReportType == 'AdgroupBudget_delta.tsv':
                dictAdgrpBudget.update({nDatadate:sFilename})
            elif sReportType == 'Keyword_full.tsv' or sReportType == 'Keyword_delta.tsv':
                dictKw.update({nDatadate:sFilename})
            elif sReportType == 'Ad_full.tsv' or sReportType == 'Ad_delta.tsv':
                dictMasterAd.update({nDatadate:sFilename})
            elif sReportType == 'AdExtension_full.tsv' or sReportType == 'AdExtension_delta.tsv':
                dictMasterAdExt.update({nDatadate:sFilename})
            elif sReportType == 'Qi_full.tsv' or sReportType == 'Qi_delta.tsv':
                dictQi.update({nDatadate:sFilename})
            else:
                self.__printDebug( 'weird Report Type! - ' + sReportType )
        
        # retrieve manual BRS info if exists
        lstBrsManualDateRange = self.__retrieveNvBrspageManualInfoPeriod(sSvAcctId, sAcctTitle, cid )
        for nManualDatadate in lstBrsManualDateRange:
            self.__g_lstDatadateToCompile.append(nManualDatadate)

        self.__g_lstDatadateToCompile = sorted(set(self.__g_lstDatadateToCompile))
        
        # register master datafile
        self.__registerMasterCampaignFile( sDataPath, sAcctTitle, dictCamp )
        self.__registerMasterCampaignBudgetFile( sDataPath, sAcctTitle, dictCampBudget )
        self.__registerMasterAdGroupFile( sDataPath, sAcctTitle, dictAdgrp )
        self.__registerMasterAdGroupBudgetFile( sDataPath, sAcctTitle, dictAdgrpBudget )
        self.__registerMasterKeywordFile( sDataPath, sAcctTitle, dictKw )
        self.__registerMasterAdFile( sDataPath, sAcctTitle, dictMasterAd )
        self.__registerMasterAdExtFile( sDataPath, sAcctTitle, dictMasterAdExt )
        self.__registerMasterQiFile( sDataPath, sAcctTitle, dictQi )
        
        # register stat datafile
        self.__registerStatAdFile( sDataPath, sAcctTitle, dictStatAd )
        self.__registerStatAdDetailFile( sDataPath, sAcctTitle, dictAdDetail )
        self.__registerStatAdConvFile( sDataPath, sAcctTitle, dictAdConversion )
        self.__registerStatAdConvDetailFile( sDataPath, sAcctTitle, dictAdConversionDetail )
        self.__registerStatAdExtFile( sDataPath, sAcctTitle, dictAdExtension )
        self.__registerStatAdExtConvFile( sDataPath, sAcctTitle, dictAdExtensionConversion )
        self.__registerStatNpayConvFile( sDataPath, sAcctTitle, dictNpayConversion )
        self.__registerStatExpKeywordFile( sDataPath, sAcctTitle, dictExpkeyword )

    def __retrieveNvBrspageManualInfoPeriod(self, sSvAcctId, sAcctTitle, cid ):
        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            lstLatestBrsLogDate = oSvMysql.executeQuery('getLatestAssembledBrsLogDate', cid )
        
        nLatestBrspageLogDate = 19700101 # set sentinel data date
        if lstLatestBrsLogDate[0]['maxdate'] is not None:
            nLatestBrspageLogDate = int( lstLatestBrsLogDate[0]['maxdate'].strftime('%Y%m%d') )
        
        lstLogPeriod = []
        sBrspagePerformanceInfoFilePath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId +'/' + sAcctTitle + '/naver_ad/' + cid + '/performance_brs_info.tsv'
        try:
            with codecs.open(sBrspagePerformanceInfoFilePath, 'r',encoding='utf8') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                nRowCnt = 0
                for row in reader:
                    if( nRowCnt > 0 ): 
                        try:
                            dtLogDate = datetime.strptime(row[0], '%Y%m%d').date()
                        except ValueError:
                            raise Exception('stop')
                            return

                        if int( row[0] ) > nLatestBrspageLogDate:
                            lstLogPeriod.append( int( row[0] ) )
                            
                    nRowCnt = nRowCnt + 1
        except FileNotFoundError:
            pass
        
        return set(lstLogPeriod)

    def __retrieveNvBrspageManualInfoByDate(self, sSvAcctId, sAcctTitle, cid, sCompileDate ):
        lstNvBrsManualInfo = []
        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            lstBrsLogSrlByDate = oSvMysql.executeQuery('getAssembledBrsLogDaily', sCompileDate, cid )
        
        if len( lstBrsLogSrlByDate ) > 0: # API brs info is primary always!
            pass
        else:
            nBrsDatadate = int(sCompileDate.replace('-',''))
            
            sBrspagePerformanceInfoFilePath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId +'/' + sAcctTitle + '/naver_ad/' + cid + '/performance_brs_info.tsv'
            try:
                with codecs.open(sBrspagePerformanceInfoFilePath, 'r',encoding='utf8') as tsvfile:
                    reader = csv.reader(tsvfile, delimiter='\t')
                    nRowCnt = 0
                    for row in reader:
                        if( nRowCnt > 0 ): 
                            try:
                                dtLogDate = datetime.strptime(row[0], '%Y%m%d').date()
                            except ValueError:
                                raise Exception('stop')
                                return

                            if int( row[0] ) == nBrsDatadate:
                                dictTempRow = {'ua':row[1], 'term':row[2], 'imp':row[3], 'click':row[4], 'conv_cnt':row[5], 'conv_amnt':row[6] }
                                lstNvBrsManualInfo.append( dictTempRow )
                                
                        nRowCnt = nRowCnt + 1
            except FileNotFoundError:
                pass
        
        return lstNvBrsManualInfo

    def __registerMasterQiFile(self, sDataPath, sAcctTitle, dictMasterData ):
        # sort master datafile dictionary by date-order
        dictMasterDataSorted = OrderedDict(sorted(dictMasterData.items()))
        nIdx = 0
        nSentinel = len(dictMasterDataSorted)
        
        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            for sDate in dictMasterDataSorted:
                sCurrentFileName = dictMasterDataSorted[sDate]
                sDataFileFullpathname = os.path.join(sDataPath, sCurrentFileName)
                m = re.search(r'^\d+', sCurrentFileName)
                
                if( len( m.group(0) ) == 8 ):
                    sCheckDate = datetime.strptime( m.group(0), "%Y%m%d" )
                elif( len( m.group(0) ) == 14 ):
                    sCheckDate = datetime.strptime( m.group(0)[0:8], "%Y%m%d" )

                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            #sCheckDate = datetime.strptime( row[0], "%Y%m%d" )
                            oSvMysql.executeQuery('insertMasterQi', row[0], row[1], row[2], row[3], row[4], sCheckDate )
                    self.__archiveNvadDataFile(sDataPath, sCurrentFileName)
                except FileNotFoundError:
                    self.__printDebug( 'pass ' + sDataFileFullpathname + ' does not exist')
                
                self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'register master qi file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerMasterAdExtFile(self, sDataPath, sAcctTitle, dictMasterData ):
        # sort master datafile dictionary by date-order
        dictMasterDataSorted = OrderedDict(sorted(dictMasterData.items()))
        nIdx = 0
        nSentinel = len(dictMasterDataSorted)

        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            for sDate in dictMasterDataSorted:
                sCurrentFileName = dictMasterDataSorted[sDate]
                sDataFileFullpathname = os.path.join(sDataPath, sCurrentFileName)
                #self.__printDebug( sDataFileFullpathname )
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            if len( row[6] ) == 0:
                                row[6] = 0
                            if len( row[7] ) == 0:
                                row[7] = 0
                            if len( row[8] ) == 0:
                                row[8] = 0
                            if len( row[9] ) == 0:
                                row[9] = 0
                            if len( row[10] ) == 0:
                                row[10] = 0
                            if len( row[11] ) == 0:
                                row[11] = 0
                            if len( row[12] ) == 0:
                                row[12] = 0

                            if len( row[15] ) > 0:
                                row[15] = re.sub(r'\.\d+', '', row[15])
                                row[15] = datetime.strptime( row[15], "%Y-%m-%dT%H:%M:%SZ" )
                            else:
                                row[15] = '0000-00-00 00:00:00'
                            if len( row[16] ) > 0:
                                row[16] = datetime.strptime( row[16], "%Y-%m-%dT%H:%M:%SZ" )
                            else:
                                row[16] = '0000-00-00 00:00:00'
                            oSvMysql.executeQuery('insertMasterAdExt', row[0], row[1], row[2], row[3], row[4], row[5],row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13],row[14], row[15], row[16] )
                    self.__archiveNvadDataFile(sDataPath, sCurrentFileName)
                except FileNotFoundError:
                    self.__printDebug( 'pass ' + sDataFileFullpathname + ' does not exist')
                
                self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'register master ad ext file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerMasterAdFile(self, sDataPath, sAcctTitle, dictMasterData ):
        # sort master datafile dictionary by date-order
        dictMasterDataSorted = OrderedDict(sorted(dictMasterData.items()))
        nIdx = 0
        nSentinel = len(dictMasterDataSorted)

        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            for sDate in dictMasterDataSorted:
                sCurrentFileName = dictMasterDataSorted[sDate]
                sDataFileFullpathname = os.path.join(sDataPath, sCurrentFileName)
                #self.__printDebug( sDataFileFullpathname )

                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            if len( row[9] ) > 0:
                                row[9] = re.sub(r'\.\d+', '', row[9])
                                row[9] = datetime.strptime( row[9], "%Y-%m-%dT%H:%M:%SZ" )
                            else:
                                row[9] = '0000-00-00 00:00:00'
                            if len( row[10] ) > 0:
                                row[10] = datetime.strptime( row[10], "%Y-%m-%dT%H:%M:%SZ" )
                            else:
                                row[10] = '0000-00-00 00:00:00'	
                            oSvMysql.executeQuery('insertMasterAd', row[0], row[1], row[2], row[3], row[4], row[5],row[6], row[7], row[8], row[9], row[10] )
                    self.__archiveNvadDataFile(sDataPath, sCurrentFileName)
                except FileNotFoundError:
                    self.__printDebug( 'pass ' + sDataFileFullpathname + ' does not exist')

                self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'register master ad file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerMasterKeywordFile(self, sDataPath, sAcctTitle, dictMasterData ):
        # sort master datafile dictionary by date-order
        dictMasterDataSorted = OrderedDict(sorted(dictMasterData.items()))
        nIdx = 0
        nSentinel = len(dictMasterDataSorted)

        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            for sDate in dictMasterDataSorted:
                sCurrentFileName = dictMasterDataSorted[sDate]
                sDataFileFullpathname = os.path.join(sDataPath, sCurrentFileName)
                #self.__printDebug( sDataFileFullpathname )

                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            if len( row[10] ) > 0:
                                row[10] = re.sub(r'\.\d+', '', row[10])
                                row[10] = datetime.strptime( row[10], "%Y-%m-%dT%H:%M:%SZ" )
                            else:
                                row[10] = '0000-00-00 00:00:00'						
                            if len( row[11] ) > 0:
                                row[11] = datetime.strptime( row[11], "%Y-%m-%dT%H:%M:%SZ" )
                            else:
                                row[11] = '0000-00-00 00:00:00'						
                            oSvMysql.executeQuery('insertMasterKeyword', row[0], row[1], row[2], row[3], row[4], row[5],row[6], row[7], row[8], row[9], row[10], row[11] )
                    self.__archiveNvadDataFile(sDataPath, sCurrentFileName)
                except FileNotFoundError:
                    self.__printDebug( 'pass ' + sDataFileFullpathname + ' does not exist')
                
                self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'register master keyword file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerMasterAdGroupBudgetFile(self, sDataPath, sAcctTitle, dictMasterData ):
        # sort master datafile dictionary by date-order
        dictMasterDataSorted = OrderedDict(sorted(dictMasterData.items()))
        nIdx = 0
        nSentinel = len(dictMasterDataSorted)

        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            for sDate in dictMasterDataSorted:
                sCurrentFileName = dictMasterDataSorted[sDate]
                sDataFileFullpathname = os.path.join(sDataPath, sCurrentFileName)
                #self.__printDebug( sDataFileFullpathname )
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            if len( row[4] ) > 0:
                                row[4] = re.sub(r'\.\d+', '', row[4])
                                row[4] = datetime.strptime( row[4], "%Y-%m-%dT%H:%M:%SZ" )
                            else:
                                row[4] = '0000-00-00 00:00:00'						
                            if len( row[5] ) > 0:
                                row[5] = datetime.strptime( row[5], "%Y-%m-%dT%H:%M:%SZ" )
                            else:
                                row[5] = '0000-00-00 00:00:00'						
                            oSvMysql.executeQuery('insertMasterAdGroupBudget', row[0], row[1], row[2], row[3], row[4], row[5] )
                    self.__archiveNvadDataFile(sDataPath, sCurrentFileName)
                except FileNotFoundError:
                    self.__printDebug( 'pass ' + sDataFileFullpathname + ' does not exist')
                
                self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'register master adgrp budget file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerMasterAdGroupFile(self, sDataPath, sAcctTitle, dictMasterData ):
        # read adgrp alias info
        sAdgrpAliasInfoFilePath = sDataPath + '/alias_adgrp_info.tsv'
        dictAdgrpAlias = {}

        try:
            with codecs.open(sAdgrpAliasInfoFilePath, 'r',encoding='utf8') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                nRowCnt = 0
                for row in reader:
                    if( nRowCnt > 0 ): 
                        dictAdgrpAlias[row[0]] = row[1]
                        
                    nRowCnt = nRowCnt + 1
        except FileNotFoundError:
            pass

        # sort master datafile dictionary by date-order
        dictMasterDataSorted = OrderedDict(sorted(dictMasterData.items()))
        
        nIdx = 0
        nSentinel = len(dictMasterDataSorted)

        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            for sDate in dictMasterDataSorted:
                sCurrentFileName = dictMasterDataSorted[sDate]
                sDataFileFullpathname = os.path.join(sDataPath, sCurrentFileName)
                #self.__printDebug( sDataFileFullpathname )
                
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                    
                        for row in reader:
                            try:
                                row[3] = dictAdgrpAlias[ row[3] ]
                            except KeyError:
                                pass

                            if( row[3].startswith( 'NV_PS_' ) ): # singleview standard case
                                # correct NV_PS_CPC_CONCENTRATION_00_00_#0002 type campaign name
                                aCampaignCode = row[3].split('_')
                                nLastPart = len(aCampaignCode ) - 1
                                
                                sCorrectedCampaignCode = ''
                                nCnt = 0
                                if( '#' in aCampaignCode[nLastPart] ):
                                    self.__printDebug( 'correct weird campaign name from NAVER AD API server ')
                                    del aCampaignCode[-1]  # remove last part that naver errornously added

                                    for sCampaignPart in aCampaignCode:
                                        sCorrectedCampaignCode += sCampaignPart
                                        if nCnt < nLastPart - 1:
                                            sCorrectedCampaignCode += '_'
                                            nCnt += 1
                                    row[3] = sCorrectedCampaignCode
                            
                            if len( row[14] ) > 0:
                                row[14] = datetime.strptime( row[14], "%Y-%m-%dT%H:%M:%SZ" )
                            else:
                                row[14] = '0000-00-00 00:00:00'						
                            if len( row[15] ) > 0:
                                row[15] = datetime.strptime( row[15], "%Y-%m-%dT%H:%M:%SZ" )
                            else:
                                row[15] = '0000-00-00 00:00:00'

                            oSvMysql.executeQuery('insertMasterAdGroup', row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]
                                , row[11], row[12], row[13], row[14], row[15])
                    self.__archiveNvadDataFile(sDataPath, sCurrentFileName)
                except FileNotFoundError:
                    self.__printDebug( 'pass ' + sDataFileFullpathname + ' does not exist')
                
                self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'register master adgrp file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerMasterCampaignFile(self, sDataPath, sAcctTitle, dictMasterData ):
        # sort master datafile dictionary by date-order
        dictMasterDataSorted = OrderedDict(sorted(dictMasterData.items()))
        
        nIdx = 0
        nSentinel = len(dictMasterDataSorted)

        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            for sDate in dictMasterDataSorted:
                sCurrentFileName = dictMasterDataSorted[sDate]
                sDataFileFullpathname = os.path.join(sDataPath, sCurrentFileName)
                #self.__printDebug( sDataFileFullpathname )

                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            if len( row[6] ) > 0:
                                row[6] = datetime.strptime( row[6], "%Y-%m-%dT%H:%M:%SZ" )
                            else:
                                row[6] = '0000-00-00 00:00:00'
                            if len( row[7] ) > 0:
                                row[7] = datetime.strptime( row[7], "%Y-%m-%dT%H:%M:%SZ" )
                            else:
                                row[7] = '0000-00-00 00:00:00'
                            if len( row[8] ) > 0:
                                row[8] = datetime.strptime( row[8], "%Y-%m-%dT%H:%M:%SZ" )
                            else:
                                row[8] = '0000-00-00 00:00:00'
                            if len( row[9] ) > 0:
                                row[9] = datetime.strptime( row[9], "%Y-%m-%dT%H:%M:%SZ" )
                            else:
                                row[9] = '0000-00-00 00:00:00'
                                                            
                            oSvMysql.executeQuery('insertMasterCampaign', row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9] )
                            #self.__printDebug( row )
                    self.__archiveNvadDataFile(sDataPath, sCurrentFileName)
                except FileNotFoundError:
                    self.__printDebug( 'pass ' + sDataFileFullpathname + ' does not exist')

                self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'register master campaign file:', suffix = 'Complete', length = 50)
                nIdx += 1
        
    def __registerMasterCampaignBudgetFile(self, sDataPath, sAcctTitle, dictMasterData ):
        # sort master datafile dictionary by date-order
        dictMasterDataSorted = OrderedDict(sorted(dictMasterData.items()))
        
        nIdx = 0
        nSentinel = len(dictMasterDataSorted)

        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            for sDate in dictMasterDataSorted:
                sCurrentFileName = dictMasterDataSorted[sDate]
                sDataFileFullpathname = os.path.join(sDataPath, sCurrentFileName)
                #self.__printDebug( sDataFileFullpathname )

                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            if len( row[4] ) > 0:
                                row[4] = datetime.strptime( row[4], "%Y-%m-%dT%H:%M:%SZ" )
                            else:
                                row[4] = '0000-00-00 00:00:00'						
                            if len( row[5] ) > 0:
                                row[5] = datetime.strptime( row[5], "%Y-%m-%dT%H:%M:%SZ" )
                            else:
                                row[5] = '0000-00-00 00:00:00'						
                            oSvMysql.executeQuery('insertMasterCampaignBudget', row[0], row[1], row[2], row[3], row[4], row[5] )
                    self.__archiveNvadDataFile(sDataPath, sCurrentFileName)
                except FileNotFoundError:
                    self.__printDebug( 'pass ' + sDataFileFullpathname + ' does not exist')
                
                self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'register master campaign budget file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerStatNpayConvFile(self, sDataPath, sAcctTitle, dictStatData ):
        # sort master datafile dictionary by date-order
        dictStatDataSorted = OrderedDict(sorted(dictStatData.items()))		
        
        nIdx = 0
        nSentinel = len(dictStatDataSorted)
        
        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            for sDate in dictStatDataSorted:
                sCurrentFileName = dictStatDataSorted[sDate]
                sDataFileFullpathname = os.path.join(sDataPath, sCurrentFileName)
                
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            try: 
                                oSvMysql.executeQuery('insertStatNpayConversion', row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9] )
                                pass
                            except Exception as err:
                                self.__printDebug( err)
                                self.__printDebug( sDataFileFullpathname)
                    self.__archiveNvadDataFile(sDataPath, sCurrentFileName)
                except FileNotFoundError:
                    self.__printDebug( 'pass ' + sDataFileFullpathname + ' does not exist')
                
                self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'register stat npay conv file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerStatAdExtConvFile(self, sDataPath, sAcctTitle, dictStatData ):
        # sort master datafile dictionary by date-order
        dictStatDataSorted = OrderedDict(sorted(dictStatData.items()))		
        
        nIdx = 0
        nSentinel = len(dictStatDataSorted)

        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            for sDate in dictStatDataSorted:
                sCurrentFileName = dictStatDataSorted[sDate]
                sDataFileFullpathname = os.path.join(sDataPath, sCurrentFileName)
                
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            try: 
                                oSvMysql.executeQuery('insertStatAdExtensionConversion', row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13] )
                                pass
                            except Exception as err:
                                self.__printDebug( err)
                                self.__printDebug( sDataFileFullpathname)
                    self.__archiveNvadDataFile(sDataPath, sCurrentFileName)
                except FileNotFoundError:
                    self.__printDebug( 'pass ' + sDataFileFullpathname + ' does not exist')
                
                self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'register stat ad ext conv file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerStatAdExtFile(self, sDataPath, sAcctTitle, dictStatData ):
        # sort master datafile dictionary by date-order
        dictStatDataSorted = OrderedDict(sorted(dictStatData.items()))		
        
        nIdx = 0
        nSentinel = len(dictStatDataSorted)

        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            for sDate in dictStatDataSorted:
                sCurrentFileName = dictStatDataSorted[sDate]
                sDataFileFullpathname = os.path.join(sDataPath, sCurrentFileName)
                
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            try: 
                                oSvMysql.executeQuery('insertStatAdExtension', row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13] )
                                pass
                            except Exception as err:
                                self.__printDebug( err)
                                self.__printDebug( sDataFileFullpathname)
                    self.__archiveNvadDataFile(sDataPath, sCurrentFileName)
                except FileNotFoundError:
                    self.__printDebug( 'pass ' + sDataFileFullpathname + ' does not exist')

                self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'register stat ad ext file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerStatAdConvDetailFile(self, sDataPath, sAcctTitle, dictStatData ):
        # sort master datafile dictionary by date-order
        dictStatDataSorted = OrderedDict(sorted(dictStatData.items()))		
        
        nIdx = 0
        nSentinel = len(dictStatDataSorted)

        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            for sDate in dictStatDataSorted:
                sCurrentFileName = dictStatDataSorted[sDate]
                sDataFileFullpathname = os.path.join(sDataPath, sCurrentFileName)
                
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            try: 
                                oSvMysql.executeQuery('insertStatAdConversionDetail', row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14] )
                                pass
                            except Exception as err:
                                self.__printDebug( err)
                                self.__printDebug( sDataFileFullpathname)
                    self.__archiveNvadDataFile(sDataPath, sCurrentFileName)
                except FileNotFoundError:
                    self.__printDebug( 'pass ' + sDataFileFullpathname + ' does not exist')
                
                self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'register stat ad conv detail file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerStatAdConvFile(self, sDataPath, sAcctTitle, dictStatData ):
        # sort master datafile dictionary by date-order
        dictStatDataSorted = OrderedDict(sorted(dictStatData.items()))		
        nIdx = 0
        nSentinel = len(dictStatDataSorted)

        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            for sDate in dictStatDataSorted:
                sCurrentFileName = dictStatDataSorted[sDate]
                sDataFileFullpathname = os.path.join(sDataPath, sCurrentFileName)
                
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            try: 
                                oSvMysql.executeQuery('insertStatAdConversion', row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12] )
                                pass
                            except Exception as err:
                                self.__printDebug( err)
                                self.__printDebug( sDataFileFullpathname)
                    self.__archiveNvadDataFile(sDataPath, sCurrentFileName)
                except FileNotFoundError:
                    self.__printDebug( 'pass ' + sDataFileFullpathname + ' does not exist')
                
                self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'register stat ad conv file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerStatAdDetailFile(self, sDataPath, sAcctTitle, dictStatData ):
        # sort master datafile dictionary by date-order
        dictStatDataSorted = OrderedDict(sorted(dictStatData.items()))		
        
        nIdx = 0
        nSentinel = len(dictStatDataSorted)
        
        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            for sDate in dictStatDataSorted:
                sCurrentFileName = dictStatDataSorted[sDate]
                sDataFileFullpathname = os.path.join(sDataPath, sCurrentFileName)
                
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            try: 
                                oSvMysql.executeQuery('insertStatAdDetail', row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14] )
                                pass
                            except Exception as err:
                                self.__printDebug( err)
                                self.__printDebug( sDataFileFullpathname)
                    self.__archiveNvadDataFile(sDataPath, sCurrentFileName)
                except FileNotFoundError:
                    self.__printDebug( 'pass ' + sDataFileFullpathname + ' does not exist')
                
                self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'register stat ad detail file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerStatAdFile(self, sDataPath, sAcctTitle, dictStatData ):
        # sort master datafile dictionary by date-order
        dictStatDataSorted = OrderedDict(sorted(dictStatData.items()))		
        
        nIdx = 0
        nSentinel = len(dictStatDataSorted)
        
        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            for sDate in dictStatDataSorted:
                sCurrentFileName = dictStatDataSorted[sDate]
                sDataFileFullpathname = os.path.join(sDataPath, sCurrentFileName)
                
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            try: 
                                #row[1] # cant index 1 if file contains the string like {"timestamp":1517205663149,"status":500,"error":"Internal Server Error","exception":"java.net.SocketException","message":"Connection reset"}
                                oSvMysql.executeQuery('insertStatAd', row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12] )
                                pass
                            except Exception as err:
                                self.__printDebug( sDataFileFullpathname)
                    self.__archiveNvadDataFile(sDataPath, sCurrentFileName)
                except FileNotFoundError:
                    self.__printDebug( 'pass ' + sDataFileFullpathname + ' does not exist')

                self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'register stat ad file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerStatExpKeywordFile(self, sDataPath, sAcctTitle, dictStatData ):
        # sort master datafile dictionary by date-order
        dictStatDataSorted = OrderedDict(sorted(dictStatData.items()))
        # enforce test data file
        #dictStatDataSorted = dict()
        #dictStatDataSorted.update({20180506:'20170203_EXPKEYWORD.tsv'})
        # enforce test data file
        import re

        nIdx = 0
        nSentinel = len(dictStatDataSorted)
        
        with sv_mysql.SvMySql('job_plugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            for sDate in dictStatDataSorted:
                sCurrentFileName = dictStatDataSorted[sDate]
                sDataFileFullpathname = os.path.join(sDataPath, sCurrentFileName)
                
                try:
                    for line in fileinput.input([sDataFileFullpathname], inplace=True): # remove any " to prevent csv.reader malfunction
                        print(line.replace('"', ''), end='')
                
                    #self.__printDebug( sDataFileFullpathname)
                    with open(sDataFileFullpathname, 'r', encoding='utf-8') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            try:
                                nCostIdx = row[9].find('.') # to prevent [invalid literal for int() with base 10: '0.0']
                                if nCostIdx == -1:
                                    nCost = int(row[9])
                                else:
                                    nCost = int(float(row[9]))

                                # some expkeyword.tsv file has been encoded as ANSI which should be encoded as UTF-8 and lead pymysql occurs Warning like: (1366, "Incorrect string value: '\\xF0\\x9F\\x92\\x90\\xEB\\xA
                                if int(row[8]) > 0 or nCost > 0:
                                    row[4] = re.sub('[^\w|ㄱ-ㅎ|ㅏ-ㅣ|가-힣|\.|%|\-|\+|\?|\||\#|\/|\*|\;]', '', row[4])
                                    oSvMysql.executeQuery('insertStatExpKeyword', row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9] )
                            except Exception as err:
                                self.__printDebug( err)
                    
                    self.__archiveNvadDataFile(sDataPath, sCurrentFileName)
                except FileNotFoundError:
                    self.__printDebug( 'pass ' + sDataFileFullpathname + ' does not exist')

                self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'register stat exp keyword file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __archiveNvadDataFile(self, sDataPath, sCurrentFileName):
        #self.__printDebug( '-> archives registered data files: ' + sCurrentFileName )
        sSourcePath = sDataPath
        if not os.path.exists(sSourcePath):
            self.__printDebug( 'error: naver_ad source directory does not exist!' )
            return
        
        sArchiveDataPath = sDataPath +'/archive'

        if not os.path.exists(sArchiveDataPath):
            os.makedirs(sArchiveDataPath)

        sSourceFilePath = os.path.join(sDataPath, sCurrentFileName)
        sArchiveDataFilePath = os.path.join(sArchiveDataPath, sCurrentFileName)
        shutil.move(sSourceFilePath, sArchiveDataFilePath)

    def __copyNvadDataFile(self, sSvAcctId, sAcctTitle, cid ):
        self.__printDebug( '-> '+ cid +' copies downloaded data files' )
        sDestPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId +'/' + sAcctTitle + '/naver_ad/' + cid
        sSourcePath = basic_config.ABSOLUTE_PATH_XE + '/files/svnvcrawl/' + sSvAcctId + '/' + sAcctTitle + '/naver_ad/' + cid
        if not os.path.exists(sSourcePath):
            self.__printDebug( 'error: naver_ad source directory does not exist!' )
            return

        if not os.path.exists(sDestPath):
            os.makedirs(sDestPath)
        
        # copy original data file in XE area to python area
        lstHandledFiles = [] # to notify web server to archived processed TSV files
        lstSrcFiles = os.listdir(sSourcePath)
        for sFilename in lstSrcFiles:
            sTruncatedFilename, sFileExt = os.path.splitext(sFilename)
            if sFileExt == '.tsv':
                sSourceFileFullname = os.path.join(sSourcePath, sFilename)
                if(os.path.isfile(sSourceFileFullname)):
                    sDestFileFullname = os.path.join(sDestPath, sFilename)
                    shutil.copy(sSourceFileFullname, sDestFileFullname)
                    lstHandledFiles.append( sFilename.replace('.tsv', '' ) )

        sTarget = ''
        nSentinel = len( lstHandledFiles ) - 1
        nIdx = 0
        nTransmissionChunkCount = 0
        for sArchivingFilename in lstHandledFiles:
            sTarget = sTarget + sArchivingFilename
            if( nIdx < nSentinel ):
                sTarget = sTarget + ';'
                nIdx = nIdx + 1
            
            if( nTransmissionChunkCount < 100 ):
                nTransmissionChunkCount = nTransmissionChunkCount + 1
            else:
                sTargetUrl = self.__g_sUrl + '?mode=archive&cid=' + cid + '&target=' + sTarget + '&f=' + self.__g_sConfigLoc
                oResp = self.__getHttpResponse( sTargetUrl )
                self.__printDebug( '-> '+ cid +' archives downloaded data files on apache server' )
                sTarget = ''
                nTransmissionChunkCount = 0
        
        self.__printDebug( '-> '+ cid +' archives remaining downloaded data files on apache server' )
        sTargetUrl = self.__g_sUrl + '?mode=archive&cid=' + cid + '&target=' + sTarget + '&f=' + self.__g_sConfigLoc
        oResp = self.__getHttpResponse( sTargetUrl )

    def __printProgressBar(self, iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '='):
        """
        Print iterations progress
        Call in a loop to create terminal progress bar
        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            length      - Optional  : character length of bar (Int)
            fill        - Optional  : bar fill character (Str)
        """
        if __name__ == '__main__': # for console debugging
            percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
            filledLength = int(length * iteration // total)
            bar = fill * filledLength + '-' * (length - filledLength)
            print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = '\r')
            # Print New Line on Complete
            if iteration == total: 
                print()

if __name__ == '__main__': # for console debugging
    dictPluginParams = {'config_loc':None, 'host_url': None, 'mode':None}
    # {'config_loc':'1/test_acct','host_url': 'http://localhost/devel/svtest' } or 
    # {'config_loc': 'http://localhost/devel/svtest','host_url': 'http://localhost/devel/svtest', 'mode':'recompile'}
    nCliParams = len(sys.argv)
    if( nCliParams > 2 ):
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
        print( 'warning! at least [config_loc] [host_url] params are required for console execution.' )