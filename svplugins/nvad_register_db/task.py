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
from datetime import datetime
import os
import shutil
import sys
from collections import OrderedDict
import csv
import re
import fileinput
import codecs
import math

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    import sv_mysql
    import sv_campaign_parser
    import sv_object, sv_api_config_parser, sv_plugin
    sys.path.append('../../conf') # singleview config
    import basic_config
else: # for platform running
    from svcommon import sv_mysql
    from svcommon import sv_campaign_parser
    from svcommon import sv_object, sv_api_config_parser, sv_plugin
    # singleview config
    from conf import basic_config # singleview config


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_sMode = None
    __g_sTblPrefix = None
    __g_sNvadDataPathAbs = None
    __g_sNvadConfPathAbs = None
    __g_lstDatadateToCompile = [] # create date list to compile NVAD DB

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        self._g_sVersion = '1.0.0'
        self._g_sLastModifiedDate = '12th, Oct 2021'
        self._g_oLogger = logging.getLogger(__name__ + ' v'+self._g_sVersion)
        self._g_dictParam.update({'mode':None})
       
    def do_task(self, o_callback):
        self.__g_sMode = self._g_dictParam['mode']

        oResp = self._task_pre_proc(o_callback)
        dict_acct_info = oResp['variables']['acct_info']
        if dict_acct_info is None:
            self._printDebug('stop -> invalid config_loc')
            return
            
        s_sv_acct_id = list(dict_acct_info.keys())[0]
        s_acct_title = dict_acct_info[s_sv_acct_id]['account_title']
        dict_nvr_ad_acct = dict_acct_info[s_sv_acct_id]['nvr_ad_acct']
        self.__g_sTblPrefix = dict_acct_info[s_sv_acct_id]['tbl_prefix']
        s_cid = dict_nvr_ad_acct['customer_id']

        self.__g_sNvadDataPathAbs = os.path.join(basic_config.ABSOLUTE_PATH_BOT, 'files', s_sv_acct_id, s_acct_title, 'naver_ad', s_cid, 'data')
        self.__g_sNvadConfPathAbs = os.path.join(basic_config.ABSOLUTE_PATH_BOT, 'files', s_sv_acct_id, s_acct_title, 'naver_ad', s_cid, 'conf')
        with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql:
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            oSvMysql.initialize()

        if self.__g_sMode == None:
            self._printDebug('-> register nvad raw data')
            self.__parseNvadDataFile(s_sv_acct_id, s_acct_title, s_cid)
        elif self.__g_sMode == 'recompile':
            with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
                oSvMysql.setTablePrefix(self.__g_sTblPrefix)
                oSvMysql.truncateTable('nvad_assembled_daily_log')
                lstStatDate = oSvMysql.executeQuery('getStatDateList')
                for dictDate in lstStatDate:
                    sCompileDate = datetime.strptime(str(dictDate['date']), '%Y-%m-%d').strftime('%Y%m%d')
                    self.__g_lstDatadateToCompile.append(int(sCompileDate))

                # retrieve manual BRS info if exists
                lstBrsManualDateRange = self.__retrieveNvBrspageManualInfoPeriod(s_sv_acct_id, s_acct_title, s_cid )
                for nManualDatadate in lstBrsManualDateRange:
                    self.__g_lstDatadateToCompile.append(nManualDatadate)

                self.__g_lstDatadateToCompile = sorted(set(self.__g_lstDatadateToCompile))
        elif self.__g_sMode == 'clear':
            self._printDebug('-> clear nvad raw data')
            with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
                oSvMysql.setTablePrefix(self.__g_sTblPrefix)
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
            if not self._continue_iteration():
                break

            b_rst = self.__compileDailyRecord(s_sv_acct_id, s_acct_title, s_cid, str(nDate))
            if not b_rst:
                self._printDebug('warning! denying assemble stat data & register!')
                break
            else:
                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'assemble stat data & register:', suffix = 'Complete', length = 50)
                nIdx += 1

        self._task_post_proc(o_callback)

    def __compileDailyRecord(self, sSvAcctId, sAcctTitle, cid, sCompileDate):
        try: # validate requsted date
            sCompileDate = datetime.strptime(sCompileDate, '%Y%m%d').strftime('%Y-%m-%d')
        except ValueError:
            self._printDebug(sCompileDate + ' is invalid date string')
            return False

        dictNvBrsPageImpByUa = {'M': 0, 'P': 0}
        oSvCampaignParser = sv_campaign_parser.svCampaignParser()
        with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            dictCampaignInfo = {}
            dictAdGrpInfo = {}
            dictKwInfo = {}
            dictCompliedDailyLog = {}
            # retrieve daily nvad media log ignore weird cid
            lstDailyMediaLog = oSvMysql.executeQuery('getDailyMediaLogs', sCompileDate, cid)

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
                        lstCampaign = oSvMysql.executeQuery('getCampaignInfo', lstMediLog['campaign_id'], sCompileDate)
                        try:
                            dictCampaignInfo[lstMediLog['campaign_id']] = lstCampaign[0]['campaign_name']
                            sTranslatedCampaignTitle = lstCampaign[0]['campaign_name']
                        except IndexError: # if campaign was existed but permanently removed or unidentified 
                            self._printDebug('-1-')
                            self._printDebug(lstCampaign)
                            self._printDebug(sCompileDate)
                            self._printDebug(lstMediLog['campaign_id'])
                            dictCampaignInfo[lstMediLog['campaign_id']] = lstMediLog['campaign_id']
                            sTranslatedCampaignTitle = lstMediLog['campaign_id']

                    try: # if ad group id already retrieved
                        sTranslatedGrpTitle = dictAdGrpInfo[lstMediLog['ad_group_id']]
                    except KeyError: # if new ad group id requested
                        lstAdGrp = oSvMysql.executeQuery('getAdGrpInfo', lstMediLog['ad_group_id'], sCompileDate)
                        try:
                            dictAdGrpInfo[lstMediLog['ad_group_id']] = lstAdGrp[0]['ad_group_name']
                            sTranslatedGrpTitle = lstAdGrp[0]['ad_group_name']
                        except IndexError: # if ad grp was existed but permanently removed or unidentified 
                            self._printDebug('-2-')
                            self._printDebug(lstAdGrp)
                            self._printDebug(sCompileDate)
                            self._printDebug(lstMediLog['ad_group_id'])
                            dictAdGrpInfo[lstMediLog['ad_group_id']] = lstMediLog['ad_group_id']
                            sTranslatedGrpTitle = lstMediLog['ad_group_id']

                    if lstMediLog['ad_keyword_id'] != '-': # ignore non keyword media eg) nvr shopping
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
            # retrieve daily nvad conversion log
            lstDailyConvLogs = oSvMysql.executeQuery('getDailyConversionLogs', sCompileDate, cid)
            for lstSingleConvlog in lstDailyConvLogs:
                sConvId = lstSingleConvlog['campaign_id']+'|@|'+lstSingleConvlog['ad_group_id']+'|@|'+lstSingleConvlog['ad_keyword_id']+'|@|'+lstSingleConvlog['pc_mobile_type'] 
                try:
                    dictCompliedDailyLog[sConvId]['conv_cnt'] += int(lstSingleConvlog['conversion_count'])
                    dictCompliedDailyLog[sConvId]['conv_amnt'] += int(lstSingleConvlog['sales_by_conversion'])
                except KeyError: # it is possible to exist conversion log without same day media impression data, in a word, supposed to be a delayed conversion
                    
                    lstCampaign = oSvMysql.executeQuery('getCampaignInfo', lstSingleConvlog['campaign_id'], sCompileDate)
                    try:
                        sTranslatedCampaignTitle = lstCampaign[0]['campaign_name']
                    except IndexError: # if campaign was existed but permanently removed or unidentified 
                        self._printDebug('-4-')
                        self._printDebug(lstCampaign)
                        self._printDebug(sCompileDate)
                        self._printDebug(lstSingleConvlog['campaign_id'])
                        sTranslatedCampaignTitle = lstSingleConvlog['campaign_id']
                    
                    lstAdGrp = oSvMysql.executeQuery('getAdGrpInfo', lstSingleConvlog['ad_group_id'], sCompileDate)
                    try:
                        sTranslatedGrpTitle = lstAdGrp[0]['ad_group_name']
                    except IndexError: # if ad grp was existed but permanently removed or unidentified 
                        self._printDebug('-5-')
                        self._printDebug(lstAdGrp)
                        self._printDebug(sCompileDate)
                        self._printDebug(lstSingleConvlog['ad_group_id'])
                        sTranslatedGrpTitle = lstSingleConvlog['ad_group_id']

                    if lstSingleConvlog['ad_keyword_id'] != '-': # ignore non keyword media eg) nvr shopping
                        lstKw = oSvMysql.executeQuery('getKwInfo', lstSingleConvlog['ad_keyword_id'], sCompileDate)
                        try:
                            sTranslatedKw = lstKw[0]['ad_keyword']
                        except IndexError: # if ad keyword was existed but permanently removed or unidentified 
                            self._printDebug('-6-')
                            self._printDebug(lstKw)
                            self._printDebug(sCompileDate)
                            self._printDebug(lstSingleConvlog['ad_keyword_id'])
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
            for s_campaign_id, dict_daily_log in dictCompliedDailyLog.items():
                dict_campaign_info = self.__parseCampaignCode(oSvMysql, dict_daily_log, sCompileDate)
                dict_daily_log['source'] = dict_campaign_info['source']
                dict_daily_log['rst_type'] = dict_campaign_info['rst_type']
                dict_daily_log['media'] = dict_campaign_info['media']
                dict_daily_log['brd'] = dict_campaign_info['brd']
                dict_daily_log['campaign_1st'] = dict_campaign_info['campaign_1st']
                dict_daily_log['campaign_2nd'] = dict_campaign_info['campaign_2nd']
                dict_daily_log['campaign_3rd'] = dict_campaign_info['campaign_3rd']
                if dict_campaign_info['campaign_1st'] == 'BRS':  # if BRS exists, sum BRS impression total
                    dictNvBrsPageImpByUa[dict_daily_log['ua']] = dictNvBrsPageImpByUa[dict_daily_log['ua']] + dict_daily_log['imp']
                    dictBrspageDailyCostRst = self.__defineNvBrspageCost(sCompileDate)
                    if dictBrspageDailyCostRst['detected'] == False: # if [contract id] is "svmanual" then dictBrsInfo[sUa] would be -1 
                        self._printDebug('warning! stop -> no matched contract_brs_info.tsv\nPlease fill in matching nvr brs info\nAnd run nvad_register_db mode=recompile again')
                        return False

                if dict_daily_log['media'] == 'CPC' and dict_daily_log['campaign_1st'].find('NVSHOP') > -1:
                    dict_daily_log['campaign_1st'] = 'NVSHOP'
                    dict_daily_log['term'] = 'nvshop'

            sLogDate = datetime.strptime(sCompileDate, "%Y-%m-%d")
            bBrsInfoFromApiExist = False # API brs info is primary always!
            # insert into DB
            for s_campaign_id, dict_daily_log in dictCompliedDailyLog.items():
                if dict_daily_log['campaign_1st'] == 'BRS': # if BRS exists, allocate cost based on impression
                    bBrsInfoFromApiExist = True
                    if dictBrspageDailyCostRst[dict_daily_log['ua']] == 0: # raise exception if BRS impression exists without contract info
                        if dictBrspageDailyCostRst['detected'] == False: # if [contract id] is "svmanual" then dictBrsInfo[sUa] would be -1 
                            self._printDebug('check brs info: BRS impression exists without contract info on ' + sCompileDate)
                            return False
                    else:
                        try:
                            nCost = int(dict_daily_log['imp']/dictNvBrsPageImpByUa[dict_daily_log['ua']] * dictBrspageDailyCostRst[dict_daily_log['ua']])
                        except ZeroDivisionError: # brs info with cost exists mistakenly
                            nCost = 0
                            self._printDebug('plz check brs info: errornous situation has been detected:')
                            self._printDebug('brs impression:' + str(dict_daily_log['imp']))
                            self._printDebug('brs impression by UA:' + str(dictNvBrsPageImpByUa[dict_daily_log['ua']]))
                            self._printDebug('brs daily cost:' + str(dictBrspageDailyCostRst[dict_daily_log['ua']]))
                else:
                    nCost = dict_daily_log['cost']

                oSvMysql.executeQuery('insertAssembledNvadLog', 
                    cid, dict_daily_log['campaign_id'], dict_daily_log['campaign_name'], dict_daily_log['group_id'],
                    dict_daily_log['ua'], dict_daily_log['kw_id'], dict_daily_log['term'], dict_daily_log['rst_type'],
                    oSvCampaignParser.getSvMediumTag(dict_daily_log['media']), dict_daily_log['brd'], 
                    dict_daily_log['campaign_1st'], dict_daily_log['campaign_2nd'], dict_daily_log['campaign_3rd'], nCost, 
                    dict_daily_log['imp'], dict_daily_log['click'], dict_daily_log['conv_cnt'], dict_daily_log['conv_amnt'], sLogDate)
            
            # register manual BRS info if allowed
            if bBrsInfoFromApiExist == False: # API brs info is primary always!
                lstNvBrsManualInfoByDate = self.__retrieveNvBrspageManualInfoByDate(sSvAcctId, sAcctTitle, cid, sCompileDate)
                for dictNvBrsManualInfo in lstNvBrsManualInfoByDate:
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
                        sRstType, sMedia, sBrd, sCamp1st, sCamp2nd, sCamp3rd, nCost,
                        dictNvBrsManualInfo['imp'], dictNvBrsManualInfo['click'], dictNvBrsManualInfo['conv_cnt'], dictNvBrsManualInfo['conv_amnt'], sLogDate )
        return True

    def __defineNvBrspageCost(self, sCompileDate):
        dictRst = {'M':0, 'P':0, 'detected':False}
        dtTouchingDate = datetime.strptime(sCompileDate, '%Y-%m-%d').date()
        sBrspageInfoFilePath = os.path.join(self.__g_sNvadConfPathAbs, 'contract_brs_info.tsv')
        try:
            with codecs.open(sBrspageInfoFilePath, 'r',encoding='utf8') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                nRowCnt = 0
                for row in reader:
                    if nRowCnt > 0:
                        if row[7] != '-':
                            aContractPeriod = row[7].split('~')
                            if len(aContractPeriod[0]) > 0: # contract date - start
                                try: # validate requsted date
                                    dtBeginDate = datetime.strptime(aContractPeriod[0], '%Y.%m.%d.').date()
                                except ValueError:
                                    self._printDebug('start date:' + aContractPeriod[0] + ' is invalid date string')

                            if len(aContractPeriod[1]) > 0: # contract date - end
                                try: # validate requsted date
                                    dtEndDate = datetime.strptime(aContractPeriod[1], '%Y.%m.%d.').date()
                                except ValueError:
                                    self._printDebug('end date:' + aContractPeriod[1] + ' is invalid date string')

                            if dtBeginDate <= dtTouchingDate and dtEndDate >= dtTouchingDate:
                                dictRst['detected'] = True  # tag brs info detected even if cost is 0
                                if row[0] != 'svmanual':
                                    dtDeltaDays = dtEndDate - dtBeginDate
                                    if row[9] == '-':
                                        nRefundAmnt = 0
                                    else:
                                        nRefundAmnt = int(row[9].replace(',', ''))
                                    nNetPeriodCost = int(row[8].replace(',', '')) - nRefundAmnt
                                    nPeriodCostExcVat = math.ceil(nNetPeriodCost / 1.1)
                                    nDailyCost = nPeriodCostExcVat / (dtDeltaDays.days + 1)
                                    sUa = row[10] # contract UA
                                    dictRst[row[10]] = dictRst[sUa] + nDailyCost
                                elif row[0] == 'svmanual':
                                    dictRst[row[10]] = -1
                        
                    nRowCnt = nRowCnt + 1
        except FileNotFoundError:
            self._printDebug('failure -> ' + sBrspageInfoFilePath + ' does not exist')
            # raise Exception('stop')
        return dictRst

    def __parseCampaignCode(self, oSvMysql, dictCompliedDailyLog, sCompileDate):
        sCampaignCode = dictCompliedDailyLog['grp_name']
        aCampaignDefinition = sCampaignCode.split('_')
        if len(aCampaignDefinition) > 5:
            sSourceTitle = aCampaignDefinition[0]
            if sSourceTitle == 'NV':
                dictCampaignInfo = {'source':'','rst_type':'','media':'','brd':0,'campaign_1st':'','campaign_2nd':'','campaign_3rd':''}
                dictCampaignInfo['source'] = sSourceTitle
                dictCampaignInfo['rst_type'] = aCampaignDefinition[1]
                dictCampaignInfo['media'] = aCampaignDefinition[2]
                
                if aCampaignDefinition[2] == 'DISP' and aCampaignDefinition[3] == 'BRS':
                    dictCampaignInfo['brd'] = 1
                if aCampaignDefinition[2] == 'CPC' and aCampaignDefinition[3] == 'NVSHOPPING':
                    dictCampaignInfo['term'] = 'nvshopping'
                if aCampaignDefinition[3] == 'BR':
                    dictCampaignInfo['brd'] = 1

                dictCampaignInfo['campaign_1st'] = aCampaignDefinition[3]
                dictCampaignInfo['campaign_2nd'] = aCampaignDefinition[4]
                dictCampaignInfo['campaign_3rd'] = aCampaignDefinition[5]
            elif sSourceTitle == 'GG':
                self._printDebug(aCampaignDefinition)
            elif sSourceTitle == 'FB':
                self._printDebug(aCampaignDefinition)
            else:
                dictCampaignInfo = {}
        else:
            lstCampaign = oSvMysql.executeQuery('getCampaignInfo', dictCompliedDailyLog['campaign_id'], sCompileDate)
            nCampaignType = lstCampaign[0]['campaign_type']
            if nCampaignType == 1:
                dictCampaignInfo = {'source':'NV','rst_type':'PS','media':'CPC','brd':0,'campaign_1st':dictCompliedDailyLog['campaign_name'], \
                    'campaign_2nd':'','campaign_3rd':''}
            elif nCampaignType == 2:
                dictCampaignInfo = {'source':'NV','rst_type':'PS','media':'CPC','brd':0,'campaign_1st':'NVSHOP', \
                    'campaign_2nd':'', 'campaign_3rd':'', 'term':'nvshop' }
            elif nCampaignType == 4:
                dictCampaignInfo = {'source':'NV','rst_type':'PS','media':'DISP','brd':1,'campaign_1st':'BRS', \
                    'campaign_2nd':dictCompliedDailyLog['grp_name'],'campaign_3rd':''}
            else:
                self._printDebug('werid campaign info')
                dictCampaignInfo = {}
        return dictCampaignInfo

    def __parseNvadDataFile(self, sSvAcctId, sAcctTitle, cid):
        self._printDebug('-> '+ cid +' is registering NVAD data files')
        # dictionary for master data file
        dictBizCh = {}
        dictCamp = {}
        dictCampBudget = {}
        dictAdgrp = {}
        dictAdgrpBudget = {}
        dictKw = {}
        dictMasterAd = {}
        dictMasterAdExt = {}
        dictQi = {}

        # dictionary for stat data file
        dictStatAd = {}
        dictAdDetail = {}
        dictAdConversion = {}
        dictAdConversionDetail = {}
        dictAdExtension = {}
        dictAdExtensionConversion = {}
        dictNpayConversion = {}
        dictExpkeyword = {}
        
        # traverse directory and categorize data files
        lstDataFiles = os.listdir(self.__g_sNvadDataPathAbs)
        for sFilename in lstDataFiles:
            if not self._continue_iteration():
                break
            
            aFileExt = os.path.splitext(sFilename)
            if aFileExt[1] == '':
                continue
            
            aFile = sFilename.split('_')
            # check a data file type
            if len(aFile[0]) == 14:
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
                dictStatAd[nDatadate] = sFilename
                self.__g_lstDatadateToCompile.append(nDatadate)
            elif sReportType == 'AD_DETAIL.tsv':
                dictAdDetail[nDatadate] = sFilename
                self.__g_lstDatadateToCompile.append(nDatadate)
            elif sReportType == 'AD_CONVERSION.tsv':
                dictAdConversion[nDatadate] = sFilename
                self.__g_lstDatadateToCompile.append(nDatadate)
            elif sReportType == 'AD_CONVERSION_DETAIL.tsv':
                dictAdConversionDetail[nDatadate] = sFilename
                self.__g_lstDatadateToCompile.append(nDatadate)
            elif sReportType == 'ADEXTENSION.tsv':
                dictAdExtension[nDatadate] = sFilename
                self.__g_lstDatadateToCompile.append(nDatadate)
            elif sReportType == 'ADEXTENSION_CONVERSION.tsv':
                dictAdExtensionConversion[nDatadate] = sFilename
                self.__g_lstDatadateToCompile.append(nDatadate)
            elif sReportType == 'NAVERPAY_CONVERSION.tsv':
                dictNpayConversion[nDatadate] = sFilename
                self.__g_lstDatadateToCompile.append(nDatadate)
            elif sReportType == 'EXPKEYWORD.tsv':
                dictExpkeyword[nDatadate] = sFilename
                self.__g_lstDatadateToCompile.append(nDatadate)
            elif sReportType == 'BusinessChannel_full.tsv' or sReportType == 'BusinessChannel_delta.tsv':
                dictBizCh[nDatadate] = sFilename
            elif sReportType == 'Campaign_full.tsv' or sReportType == 'Campaign_delta.tsv':
                dictCamp[nDatadate] = sFilename
            elif sReportType == 'CampaignBudget_full.tsv' or sReportType == 'CampaignBudget_delta.tsv':
                dictCampBudget[nDatadate] = sFilename
            elif sReportType == 'Adgroup_full.tsv' or sReportType == 'Adgroup_delta.tsv':
                dictAdgrp[nDatadate] = sFilename
            elif sReportType == 'AdgroupBudget_full.tsv' or sReportType == 'AdgroupBudget_delta.tsv':
                dictAdgrpBudget[nDatadate] = sFilename
            elif sReportType == 'Keyword_full.tsv' or sReportType == 'Keyword_delta.tsv':
                dictKw[nDatadate] = sFilename
            elif sReportType == 'Ad_full.tsv' or sReportType == 'Ad_delta.tsv':
                dictMasterAd[nDatadate] = sFilename
            elif sReportType == 'AdExtension_full.tsv' or sReportType == 'AdExtension_delta.tsv':
                dictMasterAdExt[nDatadate] = sFilename
            elif sReportType == 'Qi_full.tsv' or sReportType == 'Qi_delta.tsv':
                dictQi[nDatadate] = sFilename
            else:
                self._printDebug('weird Report Type! - ' + sReportType)
        
        # retrieve manual BRS info if exists
        lstBrsManualDateRange = self.__retrieveNvBrspageManualInfoPeriod(sSvAcctId, sAcctTitle, cid)
        for nManualDatadate in lstBrsManualDateRange:
            self.__g_lstDatadateToCompile.append(nManualDatadate)
        self.__g_lstDatadateToCompile = sorted(set(self.__g_lstDatadateToCompile))
        
        # register master datafile
        self.__registerMasterCampaignFile(sAcctTitle, dictCamp)
        self.__registerMasterCampaignBudgetFile(sAcctTitle, dictCampBudget)
        self.__registerMasterAdGroupFile(sAcctTitle, dictAdgrp)
        self.__registerMasterAdGroupBudgetFile(sAcctTitle, dictAdgrpBudget)
        self.__registerMasterKeywordFile(sAcctTitle, dictKw)
        self.__registerMasterAdFile(sAcctTitle, dictMasterAd)
        self.__registerMasterAdExtFile(sAcctTitle, dictMasterAdExt)
        self.__registerMasterQiFile(sAcctTitle, dictQi)
        
        # register stat datafile
        self.__registerStatAdFile(sAcctTitle, dictStatAd)
        self.__registerStatAdDetailFile(sAcctTitle, dictAdDetail)
        self.__registerStatAdConvFile(sAcctTitle, dictAdConversion)
        self.__registerStatAdConvDetailFile(sAcctTitle, dictAdConversionDetail)
        self.__registerStatAdExtFile(sAcctTitle, dictAdExtension)
        self.__registerStatAdExtConvFile(sAcctTitle, dictAdExtensionConversion)
        self.__registerStatNpayConvFile(sAcctTitle, dictNpayConversion)
        self.__registerStatExpKeywordFile(sAcctTitle, dictExpkeyword)

    def __retrieveNvBrspageManualInfoPeriod(self, sSvAcctId, sAcctTitle, cid):
        with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            lstLatestBrsLogDate = oSvMysql.executeQuery('getLatestAssembledBrsLogDate', cid)
        
        nLatestBrspageLogDate = 19700101 # set sentinel data date
        if lstLatestBrsLogDate[0]['maxdate'] is not None:
            nLatestBrspageLogDate = int(lstLatestBrsLogDate[0]['maxdate'].strftime('%Y%m%d'))
        
        lstLogPeriod = []
        sBrspagePerformanceInfoFilePath = os.path.join(self.__g_sNvadConfPathAbs, 'performance_brs_info.tsv')
        try:
            with codecs.open(sBrspagePerformanceInfoFilePath, 'r',encoding='utf8') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                nRowCnt = 0
                for row in reader:
                    if nRowCnt > 0:
                        try:
                            dtLogDate = datetime.strptime(row[0], '%Y%m%d').date()
                        except ValueError:
                            # raise Exception('stop')
                            return

                        if int(row[0]) > nLatestBrspageLogDate:
                            lstLogPeriod.append(int(row[0]))
                            
                    nRowCnt = nRowCnt + 1
        except FileNotFoundError:
            pass
        
        return set(lstLogPeriod)

    def __retrieveNvBrspageManualInfoByDate(self, sSvAcctId, sAcctTitle, cid, sCompileDate):
        lstNvBrsManualInfo = []
        with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            lstBrsLogSrlByDate = oSvMysql.executeQuery('getAssembledBrsLogDaily', sCompileDate, cid)
        
        if len(lstBrsLogSrlByDate) > 0: # API brs info is primary always!
            pass
        else:
            nBrsDatadate = int(sCompileDate.replace('-',''))
            sBrspagePerformanceInfoFilePath = os.path.join(self.__g_sNvadConfPathAbs, 'performance_brs_info.tsv')
            try:
                with codecs.open(sBrspagePerformanceInfoFilePath, 'r',encoding='utf8') as tsvfile:
                    reader = csv.reader(tsvfile, delimiter='\t')
                    nRowCnt = 0
                    for row in reader:
                        if nRowCnt > 0:
                            try:
                                dtLogDate = datetime.strptime(row[0], '%Y%m%d').date()
                            except ValueError:
                                return

                            if int(row[0]) == nBrsDatadate:
                                dictTempRow = {'ua':row[1], 'term':row[2], 'imp':row[3], 'click':row[4], 'conv_cnt':row[5], 'conv_amnt':row[6] }
                                lstNvBrsManualInfo.append(dictTempRow)
                        nRowCnt = nRowCnt + 1
            except FileNotFoundError:
                pass
        
        return lstNvBrsManualInfo

    def __registerMasterQiFile(self, sAcctTitle, dictMasterData):
        # sort master datafile dictionary by date-order
        dictMasterDataSorted = OrderedDict(sorted(dictMasterData.items()))
        nIdx = 0
        nSentinel = len(dictMasterDataSorted)
        with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            for sDate in dictMasterDataSorted:
                sCurrentFileName = dictMasterDataSorted[sDate]
                sDataFileFullpathname = os.path.join(self.__g_sNvadDataPathAbs, sCurrentFileName)
                m = re.search(r'^\d+', sCurrentFileName)
                
                if len(m.group(0)) == 8:
                    sCheckDate = datetime.strptime(m.group(0), "%Y%m%d")
                elif len(m.group(0)) == 14:
                    sCheckDate = datetime.strptime(m.group(0)[0:8], "%Y%m%d")

                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            #sCheckDate = datetime.strptime( row[0], "%Y%m%d" )
                            oSvMysql.executeQuery('insertMasterQi', row[0], row[1], row[2], row[3], row[4], sCheckDate)
                    self.__archiveNvadDataFile(self.__g_sNvadDataPathAbs, sCurrentFileName)
                except FileNotFoundError:
                    self._printDebug('pass ' + sDataFileFullpathname + ' does not exist')
                
                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'register master qi file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerMasterAdExtFile(self, sAcctTitle, dictMasterData):
        # sort master datafile dictionary by date-order
        dictMasterDataSorted = OrderedDict(sorted(dictMasterData.items()))
        nIdx = 0
        nSentinel = len(dictMasterDataSorted)
        with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            for sDate in dictMasterDataSorted:
                sCurrentFileName = dictMasterDataSorted[sDate]
                sDataFileFullpathname = os.path.join(self.__g_sNvadDataPathAbs, sCurrentFileName)
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            if len(row[6]) == 0:  # None if 'downloadUrl' not in s else s['downloadUrl']
                                row[6] = 0
                            if len(row[7]) == 0:
                                row[7] = 0
                            if len(row[8]) == 0:
                                row[8] = 0
                            if len(row[9]) == 0:
                                row[9] = 0
                            if len(row[10]) == 0:
                                row[10] = 0
                            if len(row[11]) == 0:
                                row[11] = 0
                            if len(row[12]) == 0:
                                row[12] = 0
                            if len(row[15]) > 0:
                                row[15] = re.sub(r'\.\d+', '', row[15])
                                row[15] = datetime.strptime( row[15], "%Y-%m-%dT%H:%M:%SZ")
                            else:
                                row[15] = '0000-00-00 00:00:00'
                            if len(row[16]) > 0:
                                row[16] = datetime.strptime( row[16], "%Y-%m-%dT%H:%M:%SZ")
                            else:
                                row[16] = '0000-00-00 00:00:00'
                            oSvMysql.executeQuery('insertMasterAdExt', row[0], row[1], row[2], row[3], row[4], row[5],row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13],row[14], row[15], row[16])
                    self.__archiveNvadDataFile(self.__g_sNvadDataPathAbs, sCurrentFileName)
                except FileNotFoundError:
                    self._printDebug('pass ' + sDataFileFullpathname + ' does not exist')
                
                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'register master ad ext file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerMasterAdFile(self, sAcctTitle, dictMasterData):
        # sort master datafile dictionary by date-order
        dictMasterDataSorted = OrderedDict(sorted(dictMasterData.items()))
        nIdx = 0
        nSentinel = len(dictMasterDataSorted)
        with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            for sDate in dictMasterDataSorted:
                sCurrentFileName = dictMasterDataSorted[sDate]
                sDataFileFullpathname = os.path.join(self.__g_sNvadDataPathAbs, sCurrentFileName)
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            if len(row[9]) > 0:
                                row[9] = re.sub(r'\.\d+', '', row[9])
                                row[9] = datetime.strptime(row[9], "%Y-%m-%dT%H:%M:%SZ")
                            else:
                                row[9] = '0000-00-00 00:00:00'
                            if len(row[10]) > 0:
                                row[10] = datetime.strptime( row[10], "%Y-%m-%dT%H:%M:%SZ")
                            else:
                                row[10] = '0000-00-00 00:00:00'
                            oSvMysql.executeQuery('insertMasterAd', row[0], row[1], row[2], row[3], row[4], row[5],row[6], row[7], row[8], row[9], row[10])
                    self.__archiveNvadDataFile(self.__g_sNvadDataPathAbs, sCurrentFileName)
                except FileNotFoundError:
                    self.printDebug('pass ' + sDataFileFullpathname + ' does not exist')

                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'register master ad file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerMasterKeywordFile(self, sAcctTitle, dictMasterData):
        # sort master datafile dictionary by date-order
        dictMasterDataSorted = OrderedDict(sorted(dictMasterData.items()))
        nIdx = 0
        nSentinel = len(dictMasterDataSorted)
        with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            for sDate in dictMasterDataSorted:
                sCurrentFileName = dictMasterDataSorted[sDate]
                sDataFileFullpathname = os.path.join(self.__g_sNvadDataPathAbs, sCurrentFileName)
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            if len(row[10]) > 0:
                                row[10] = re.sub(r'\.\d+', '', row[10])
                                row[10] = datetime.strptime( row[10], "%Y-%m-%dT%H:%M:%SZ" )
                            else:
                                row[10] = '0000-00-00 00:00:00'						
                            if len(row[11]) > 0:
                                row[11] = datetime.strptime( row[11], "%Y-%m-%dT%H:%M:%SZ" )
                            else:
                                row[11] = '0000-00-00 00:00:00'						
                            oSvMysql.executeQuery('insertMasterKeyword', row[0], row[1], row[2], row[3], row[4], row[5],row[6], row[7], row[8], row[9], row[10], row[11])
                    self.__archiveNvadDataFile(self.__g_sNvadDataPathAbs, sCurrentFileName)
                except FileNotFoundError:
                    self._printDebug('pass ' + sDataFileFullpathname + ' does not exist')
                
                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'register master keyword file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerMasterAdGroupBudgetFile(self, sAcctTitle, dictMasterData):
        # sort master datafile dictionary by date-order
        dictMasterDataSorted = OrderedDict(sorted(dictMasterData.items()))
        nIdx = 0
        nSentinel = len(dictMasterDataSorted)
        with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            for sDate in dictMasterDataSorted:
                sCurrentFileName = dictMasterDataSorted[sDate]
                sDataFileFullpathname = os.path.join(self.__g_sNvadDataPathAbs, sCurrentFileName)
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            if len(row[4]) > 0:
                                row[4] = re.sub(r'\.\d+', '', row[4])
                                row[4] = datetime.strptime( row[4], "%Y-%m-%dT%H:%M:%SZ")
                            else:
                                row[4] = '0000-00-00 00:00:00'						
                            if len(row[5]) > 0:
                                row[5] = datetime.strptime( row[5], "%Y-%m-%dT%H:%M:%SZ")
                            else:
                                row[5] = '0000-00-00 00:00:00'						
                            oSvMysql.executeQuery('insertMasterAdGroupBudget', row[0], row[1], row[2], row[3], row[4], row[5])
                    self.__archiveNvadDataFile(self.__g_sNvadDataPathAbs, sCurrentFileName)
                except FileNotFoundError:
                    self._printDebug('pass ' + sDataFileFullpathname + ' does not exist')
                
                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'register master adgrp budget file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerMasterAdGroupFile(self, sAcctTitle, dictMasterData):
        # read adgrp alias info
        # sAdgrpAliasInfoFilePath = sDataPath + '/alias_adgrp_info.tsv'
        sAdgrpAliasInfoFilePath = os.path.join(self.__g_sNvadDataPathAbs, 'alias_adgrp_info.tsv')
        dictAdgrpAlias = {}
        try:
            with codecs.open(sAdgrpAliasInfoFilePath, 'r',encoding='utf8') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                nRowCnt = 0
                for row in reader:
                    if nRowCnt > 0:
                        dictAdgrpAlias[row[0]] = row[1]
                    nRowCnt = nRowCnt + 1
        except FileNotFoundError:
            pass

        # sort master datafile dictionary by date-order
        dictMasterDataSorted = OrderedDict(sorted(dictMasterData.items()))
        nIdx = 0
        nSentinel = len(dictMasterDataSorted)
        with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            for sDate in dictMasterDataSorted:
                sCurrentFileName = dictMasterDataSorted[sDate]
                sDataFileFullpathname = os.path.join(self.__g_sNvadDataPathAbs, sCurrentFileName)
                #self._printDebug( sDataFileFullpathname )
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            try:
                                row[3] = dictAdgrpAlias[row[3]]
                            except KeyError:
                                pass

                            if row[3].startswith('NV_PS_'): # singleview standard case
                                # correct NV_PS_CPC_CONCENTRATION_00_00_#0002 type campaign name
                                aCampaignCode = row[3].split('_')
                                nLastPart = len(aCampaignCode ) - 1
                                sCorrectedCampaignCode = ''
                                nCnt = 0
                                if '#' in aCampaignCode[nLastPart]:
                                    self._printDebug('correct weird campaign name from NAVER AD API server ')
                                    del aCampaignCode[-1]  # remove last part that naver errornously added

                                    for sCampaignPart in aCampaignCode:
                                        sCorrectedCampaignCode += sCampaignPart
                                        if nCnt < nLastPart - 1:
                                            sCorrectedCampaignCode += '_'
                                            nCnt += 1
                                    row[3] = sCorrectedCampaignCode
                            
                            if len(row[14]) > 0:
                                row[14] = datetime.strptime( row[14], "%Y-%m-%dT%H:%M:%SZ")
                            else:
                                row[14] = '0000-00-00 00:00:00'						
                            if len(row[15]) > 0:
                                row[15] = datetime.strptime( row[15], "%Y-%m-%dT%H:%M:%SZ")
                            else:
                                row[15] = '0000-00-00 00:00:00'

                            oSvMysql.executeQuery('insertMasterAdGroup', row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]
                                , row[11], row[12], row[13], row[14], row[15])
                    self.__archiveNvadDataFile(self.__g_sNvadDataPathAbs, sCurrentFileName)
                except FileNotFoundError:
                    self._printDebug('pass ' + sDataFileFullpathname + ' does not exist')
                
                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'register master adgrp file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerMasterCampaignFile(self, sAcctTitle, dictMasterData):
        # sort master datafile dictionary by date-order
        dictMasterDataSorted = OrderedDict(sorted(dictMasterData.items()))
        nIdx = 0
        nSentinel = len(dictMasterDataSorted)
        with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            for sDate in dictMasterDataSorted:
                if not self._continue_iteration():
                    break

                sCurrentFileName = dictMasterDataSorted[sDate]
                sDataFileFullpathname = os.path.join(self.__g_sNvadDataPathAbs, sCurrentFileName)
                #self._printDebug( sDataFileFullpathname )
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            if len(row[6]) > 0:
                                row[6] = datetime.strptime(row[6], "%Y-%m-%dT%H:%M:%SZ")
                            else:
                                row[6] = '0000-00-00 00:00:00'
                            if len(row[7]) > 0:
                                row[7] = datetime.strptime(row[7], "%Y-%m-%dT%H:%M:%SZ")
                            else:
                                row[7] = '0000-00-00 00:00:00'
                            if len(row[8]) > 0:
                                row[8] = datetime.strptime(row[8], "%Y-%m-%dT%H:%M:%SZ")
                            else:
                                row[8] = '0000-00-00 00:00:00'
                            if len(row[9]) > 0:
                                row[9] = datetime.strptime(row[9], "%Y-%m-%dT%H:%M:%SZ")
                            else:
                                row[9] = '0000-00-00 00:00:00'
                                                            
                            oSvMysql.executeQuery('insertMasterCampaign', row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
                            #self._printDebug( row )
                    self.__archiveNvadDataFile(self.__g_sNvadDataPathAbs, sCurrentFileName)
                except FileNotFoundError:
                    self._printDebug('pass ' + sDataFileFullpathname + ' does not exist')

                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'register master campaign file:', suffix = 'Complete', length = 50)
                nIdx += 1
        
    def __registerMasterCampaignBudgetFile(self, sAcctTitle, dictMasterData):
        # sort master datafile dictionary by date-order
        dictMasterDataSorted = OrderedDict(sorted(dictMasterData.items()))
        nIdx = 0
        nSentinel = len(dictMasterDataSorted)
        with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            for sDate in dictMasterDataSorted:
                if not self._continue_iteration():
                    break

                sCurrentFileName = dictMasterDataSorted[sDate]
                sDataFileFullpathname = os.path.join(self.__g_sNvadDataPathAbs, sCurrentFileName)
                #self._printDebug( sDataFileFullpathname )
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            if len(row[4]) > 0:
                                row[4] = datetime.strptime(row[4], "%Y-%m-%dT%H:%M:%SZ")
                            else:
                                row[4] = '0000-00-00 00:00:00'						
                            if len(row[5]) > 0:
                                row[5] = datetime.strptime(row[5], "%Y-%m-%dT%H:%M:%SZ")
                            else:
                                row[5] = '0000-00-00 00:00:00'						
                            oSvMysql.executeQuery('insertMasterCampaignBudget', row[0], row[1], row[2], row[3], row[4], row[5])
                    self.__archiveNvadDataFile(self.__g_sNvadDataPathAbs, sCurrentFileName)
                except FileNotFoundError:
                    self._printDebug('pass ' + sDataFileFullpathname + ' does not exist')
                
                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'register master campaign budget file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerStatNpayConvFile(self, sAcctTitle, dictStatData):
        # sort master datafile dictionary by date-order
        dictStatDataSorted = OrderedDict(sorted(dictStatData.items()))		
        nIdx = 0
        nSentinel = len(dictStatDataSorted)
        with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            for sDate in dictStatDataSorted:
                if not self._continue_iteration():
                    break

                sCurrentFileName = dictStatDataSorted[sDate]
                sDataFileFullpathname = os.path.join(self.__g_sNvadDataPathAbs, sCurrentFileName)
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            try: 
                                oSvMysql.executeQuery('insertStatNpayConversion', row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
                                pass
                            except Exception as err:
                                self._printDebug(err)
                                self._printDebug(sDataFileFullpathname)
                    self.__archiveNvadDataFile(self.__g_sNvadDataPathAbs, sCurrentFileName)
                except FileNotFoundError:
                    self._printDebug( 'pass ' + sDataFileFullpathname + ' does not exist')
                
                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'register stat npay conv file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerStatAdExtConvFile(self, sAcctTitle, dictStatData):
        # sort master datafile dictionary by date-order
        dictStatDataSorted = OrderedDict(sorted(dictStatData.items()))		
        nIdx = 0
        nSentinel = len(dictStatDataSorted)
        with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            for sDate in dictStatDataSorted:
                if not self._continue_iteration():
                    break
                
                sCurrentFileName = dictStatDataSorted[sDate]
                sDataFileFullpathname = os.path.join(self.__g_sNvadDataPathAbs, sCurrentFileName)
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            try: 
                                oSvMysql.executeQuery('insertStatAdExtensionConversion', row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13] )
                                pass
                            except Exception as err:
                                self._printDebug(err)
                                self._printDebug(sDataFileFullpathname)
                    self.__archiveNvadDataFile(self.__g_sNvadDataPathAbs, sCurrentFileName)
                except FileNotFoundError:
                    self._printDebug('pass ' + sDataFileFullpathname + ' does not exist')
                
                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'register stat ad ext conv file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerStatAdExtFile(self, sAcctTitle, dictStatData):
        # sort master datafile dictionary by date-order
        dictStatDataSorted = OrderedDict(sorted(dictStatData.items()))		
        nIdx = 0
        nSentinel = len(dictStatDataSorted)
        with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            for sDate in dictStatDataSorted:
                if not self._continue_iteration():
                    break
                
                sCurrentFileName = dictStatDataSorted[sDate]
                sDataFileFullpathname = os.path.join(self.__g_sNvadDataPathAbs, sCurrentFileName)
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            try: 
                                oSvMysql.executeQuery('insertStatAdExtension', row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13])
                                pass
                            except Exception as err:
                                self._printDebug(err)
                                self._printDebug(sDataFileFullpathname)
                    self.__archiveNvadDataFile(self.__g_sNvadDataPathAbs, sCurrentFileName)
                except FileNotFoundError:
                    self._printDebug('pass ' + sDataFileFullpathname + ' does not exist')

                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'register stat ad ext file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerStatAdConvDetailFile(self, sAcctTitle, dictStatData):
        # sort master datafile dictionary by date-order
        dictStatDataSorted = OrderedDict(sorted(dictStatData.items()))		
        nIdx = 0
        nSentinel = len(dictStatDataSorted)
        with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            for sDate in dictStatDataSorted:
                if not self._continue_iteration():
                    break
                
                sCurrentFileName = dictStatDataSorted[sDate]
                sDataFileFullpathname = os.path.join(self.__g_sNvadDataPathAbs, sCurrentFileName)
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            try: 
                                oSvMysql.executeQuery('insertStatAdConversionDetail', row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14] )
                                pass
                            except Exception as err:
                                self._printDebug(err)
                                self._printDebug(sDataFileFullpathname)
                    self.__archiveNvadDataFile(self.__g_sNvadDataPathAbs, sCurrentFileName)
                except FileNotFoundError:
                    self._printDebug('pass ' + sDataFileFullpathname + ' does not exist')
                
                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'register stat ad conv detail file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerStatAdConvFile(self, sAcctTitle, dictStatData):
        # sort master datafile dictionary by date-order
        dictStatDataSorted = OrderedDict(sorted(dictStatData.items()))		
        nIdx = 0
        nSentinel = len(dictStatDataSorted)
        with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            for sDate in dictStatDataSorted:
                if not self._continue_iteration():
                    break
                
                sCurrentFileName = dictStatDataSorted[sDate]
                sDataFileFullpathname = os.path.join(self.__g_sNvadDataPathAbs, sCurrentFileName)
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            try: 
                                oSvMysql.executeQuery('insertStatAdConversion', row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12] )
                                pass
                            except Exception as err:
                                self._printDebug(err)
                                self._printDebug(sDataFileFullpathname)
                    self.__archiveNvadDataFile(self.__g_sNvadDataPathAbs, sCurrentFileName)
                except FileNotFoundError:
                    self._printDebug('pass ' + sDataFileFullpathname + ' does not exist')
                
                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'register stat ad conv file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerStatAdDetailFile(self, sAcctTitle, dictStatData):
        # sort master datafile dictionary by date-order
        dictStatDataSorted = OrderedDict(sorted(dictStatData.items()))		
        nIdx = 0
        nSentinel = len(dictStatDataSorted)
        with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            for sDate in dictStatDataSorted:
                if not self._continue_iteration():
                    break
                
                sCurrentFileName = dictStatDataSorted[sDate]
                sDataFileFullpathname = os.path.join(self.__g_sNvadDataPathAbs, sCurrentFileName)
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            try: 
                                oSvMysql.executeQuery('insertStatAdDetail', row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14] )
                                pass
                            except Exception as err:
                                self._printDebug(err)
                                self._printDebug(sDataFileFullpathname)
                    self.__archiveNvadDataFile(self.__g_sNvadDataPathAbs, sCurrentFileName)
                except FileNotFoundError:
                    self._printDebug('pass ' + sDataFileFullpathname + ' does not exist')
                
                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'register stat ad detail file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerStatAdFile(self, sAcctTitle, dictStatData):
        # sort master datafile dictionary by date-order
        dictStatDataSorted = OrderedDict(sorted(dictStatData.items()))		
        nIdx = 0
        nSentinel = len(dictStatDataSorted)
        with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            for sDate in dictStatDataSorted:
                if not self._continue_iteration():
                    break
                
                sCurrentFileName = dictStatDataSorted[sDate]
                sDataFileFullpathname = os.path.join(self.__g_sNvadDataPathAbs, sCurrentFileName)
                try:
                    with open(sDataFileFullpathname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t')
                        for row in reader:
                            try: 
                                #row[1] # cant index 1 if file contains the string like {"timestamp":1517205663149,"status":500,"error":"Internal Server Error","exception":"java.net.SocketException","message":"Connection reset"}
                                oSvMysql.executeQuery('insertStatAd', row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12])
                                pass
                            except Exception as err:
                                self._printDebug( sDataFileFullpathname)
                    self.__archiveNvadDataFile(self.__g_sNvadDataPathAbs, sCurrentFileName)
                except FileNotFoundError:
                    self._printDebug('pass ' + sDataFileFullpathname + ' does not exist')

                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'register stat ad file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __registerStatExpKeywordFile(self, sAcctTitle, dictStatData):
        # sort master datafile dictionary by date-order
        dictStatDataSorted = OrderedDict(sorted(dictStatData.items()))
        nIdx = 0
        nSentinel = len(dictStatDataSorted)
        # enforce test data file
        #dictStatDataSorted = dict()
        #dictStatDataSorted.update({20180506:'20170203_EXPKEYWORD.tsv'})
        # enforce test data file
        with sv_mysql.SvMySql('svplugins.nvad_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            for sDate in dictStatDataSorted:
                if not self._continue_iteration():
                    break
                
                sCurrentFileName = dictStatDataSorted[sDate]
                sDataFileFullpathname = os.path.join(self.__g_sNvadDataPathAbs, sCurrentFileName)
                try:
                    for line in fileinput.input([sDataFileFullpathname], inplace=True): # remove any " to prevent csv.reader malfunction
                        print(line.replace('"', ''), end='')
                
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
                                self._printDebug( err)
                    
                    self.__archiveNvadDataFile(self.__g_sNvadDataPathAbs, sCurrentFileName)
                except FileNotFoundError:
                    self._printDebug('pass ' + sDataFileFullpathname + ' does not exist')

                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'register stat exp keyword file:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __archiveNvadDataFile(self, sDataPath, sCurrentFileName):
        sSourcePath = sDataPath
        if not os.path.exists(sSourcePath):
            self._printDebug('error: naver_ad source directory does not exist!')
            return
        sArchiveDataPath = os.path.join(sDataPath, 'archive')
        if not os.path.exists(sArchiveDataPath):
            os.makedirs(sArchiveDataPath)

        sSourceFilePath = os.path.join(sDataPath, sCurrentFileName)
        sArchiveDataFilePath = os.path.join(sArchiveDataPath, sCurrentFileName)
        shutil.move(sSourceFilePath, sArchiveDataFilePath)


if __name__ == '__main__': # for console debugging
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('aw_register_db')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [analytical_namespace] [config_loc] params are required for console execution.')
