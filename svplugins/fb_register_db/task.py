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
import sys
import os
import shutil
import csv
import codecs

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svload/pandas_plugins')
    sys.path.append('../../svdjango')
    import sv_mysql
    import sv_campaign_parser
    import sv_object
    import sv_plugin
    import campaign_alias
    import settings
else: # for platform running
    from svcommon import sv_mysql
    from svcommon import sv_campaign_parser
    from svcommon import sv_object
    from svcommon import sv_plugin
    from svload.pandas_plugins import campaign_alias
    from django.conf import settings


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_oSvCampaignParser = sv_campaign_parser.SvCampaignParser()

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name+'(20221008)')
        
        # Declaring a dict outside of __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        self.__g_sTblPrefix = None
        self.__g_dictFbRaw = None  # prevent duplication on a web console
        self.__g_dictFxCodeByBizAcct = {}
        self.__g_dictFxTrendInfo = {}

    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        self.__g_sTblPrefix = None
        self.__g_dictFbRaw = None  # prevent duplication on a web console
        self.__g_dictFxCodeByBizAcct = {}
        self.__g_dictFxTrendInfo = {}

    def do_task(self, o_callback):
        self._g_oCallback = o_callback
        self.__g_dictFbRaw = {}  # prevent duplication on a web console

        dict_acct_info = self._task_pre_proc(o_callback)
        if 'sv_account_id' not in dict_acct_info and 'brand_id' not in dict_acct_info and \
          'fb_biz_aid' not in dict_acct_info:
            self._printDebug('stop -> invalid config_loc')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        s_sv_acct_id = dict_acct_info['sv_account_id']
        s_brand_id = dict_acct_info['brand_id']
        self.__g_sTblPrefix = dict_acct_info['tbl_prefix']
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.fb_register_db')
            o_sv_mysql.initialize(self._g_dictSvAcctInfo)
        # begin - referring to raw_data_file, validate raw data file without registration
        lst_non_sv_convention_campaign_title = self.__validate_fb_raw_data_file(s_sv_acct_id, s_brand_id)
        if len(lst_non_sv_convention_campaign_title):
            for s_single_campaign in lst_non_sv_convention_campaign_title:
                self._printDebug('[' + s_single_campaign + '] should be filled!')
            self._task_post_proc(self._g_oCallback)
            return False
        # end - referring to raw_data_file, validate raw data file without registration
        self.__arrange_fb_raw_data_file(s_sv_acct_id, s_brand_id)
        self.__register_db()
        self._task_post_proc(self._g_oCallback)

    def __get_fx_rate(self, sCheckFxCode, sCheckDate):
        # https://developers.facebook.com/docs/marketing-api/currencies/
        if sCheckFxCode == 'KRW':
            return 1
        dtCheckDate = datetime.strptime(sCheckDate, '%Y%m%d').date()
        for sFxIdx in self.__g_dictFxTrendInfo:
            aFxPeriodInfo = sFxIdx.split('_')
            sFxCode = aFxPeriodInfo[0]
            if sCheckFxCode == sFxCode:
                aFxPeriod = aFxPeriodInfo[1].split('~')
                if len(aFxPeriod[0]) > 0: # fx date - start
                    try: # validate requsted date
                        dtBeginDate = datetime.strptime(aFxPeriod[0], '%Y.%m.%d').date()
                    except ValueError:
                        self._printDebug('start date:' + aFxPeriod[0] + ' is invalid date string')
                else:
                    dtBeginDate = datetime(2010, 1, 1).date() # set sentinel
                    self._printDebug(dtBeginDate)

                if len(aFxPeriod[1]) > 0: # fx date - end
                    try: # validate requsted date
                        dtEndDate = datetime.strptime(aFxPeriod[1], '%Y.%m.%d').date()
                    except ValueError:
                        self._printDebug('end date:' + aFxPeriod[1] + ' is invalid date string')
                else:
                    dtEndDate = datetime(2910, 12, 31).date() # set sentinel
                
                if dtBeginDate <= dtCheckDate and dtEndDate >= dtCheckDate:
                    return self.__g_dictFxTrendInfo[sFxIdx]
        return

    def __get_fx_trend(self, sFxCode):
        # https://developers.facebook.com/docs/marketing-api/currencies/
        if sFxCode == 'KRW':
            return True
        sCurrencyTrendPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, 'info_fx_' + sFxCode + '.tsv')
        if os.path.isfile(sCurrencyTrendPath):
            with open(sCurrencyTrendPath, 'r') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                nRowCnt = 0
                for row in reader:
                    if nRowCnt > 0: 
                        self.__g_dictFxTrendInfo[sFxCode + '_' + row[0]] = int(row[1].replace(',',''))
                    nRowCnt = nRowCnt + 1
        else:
            self._printDebug('file ' + sCurrencyTrendPath + ' does not exist')
            return False
        return True

    def __get_fx_code(self, s_conf_path_abs):
        # https://developers.facebook.com/docs/marketing-api/currencies/
        sFxCodePath = os.path.join(s_conf_path_abs, 'info_fx.tsv')
        sFxCode = 'error'
        if os.path.isfile(sFxCodePath):
            with open(sFxCodePath, 'r') as f:
                sFxCode = f.readline().strip().upper()
        else:
            self._printDebug('file ' + sFxCodePath + ' does not exist')
        return sFxCode

    def __validate_fb_raw_data_file(self, s_sv_acct_id, s_brand_id):
        """ referring to raw_data_file, validate raw data file without registration """
        lst_non_sv_convention_campaign_title = []
        sDataPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_sv_acct_id, s_brand_id, 'fb_biz')
        # traverse directory and categorize data files
        lstTotalDataset = []
        lstFbBizAid = os.listdir(sDataPath)
        for sFbBizAid in lstFbBizAid:
            if sFbBizAid == 'alias_info_campaign.tsv':
                continue
            sDownloadDataPath = os.path.join(sDataPath, sFbBizAid, 'data')
            self._printDebug('-> '+ sFbBizAid +' is validating FB IG data files')
            lstDataFiles = os.listdir(sDownloadDataPath)
            for sFilename in lstDataFiles:
                aFileExt = os.path.splitext(sFilename)
                if aFileExt[1] == '':
                    continue
                sDatafileTobeHandled = sFilename + '|@|' + sFbBizAid
                lstTotalDataset.append(sDatafileTobeHandled)
        lstTotalDataset.sort()
        o_campaign_alias = campaign_alias.CampaignAliasInfo(s_sv_acct_id, s_brand_id)
        nIdx = 0
        nSentinel = len(lstTotalDataset)
        for sFileInfo in lstTotalDataset:
            aFileInfo = sFileInfo.split('|@|')
            sFbBusinessAcctId = aFileInfo[1] 
            sTempDataPath = os.path.join(sDataPath, sFbBusinessAcctId, 'data')
            sDataFileFullname = os.path.join(sTempDataPath, aFileInfo[0])
            if os.path.isfile(sDataFileFullname):
                with open(sDataFileFullname, 'r') as tsvfile:
                    reader = csv.reader(tsvfile, delimiter='\t')
                    for row in reader:
                        # row[0]: 'ad_id'
                        # row[1]: 'configured_status'
                        # row[2]: 'creative_id'
                        # row[3]: 'ad_name'
                        # row[4]: 'link'
                        # row[5]: 'url_tags'
                        # row[6]: 'device_platform'
                        # row[7]: 'reach'
                        # row[8]: 'impressions'
                        # row[9]: 'clicks'
                        # row[10]: 'unique_clicks
                        # row[11]: 'spend'
                        # row[12]: nConversionValue
                        # row[13]: nConversionCount
                        dictCampaignInfo = {'url_tags':row[5], 'ad_name':row[3], 'campaign_code':''}
                        lstUtmParams = row[5].split('&')
                        for sUtmParam in lstUtmParams:
                            if sUtmParam.find('utm_campaign=') > -1:
                                aUtmCampaignCode = sUtmParam.split('=')
                                dictCampaignInfo['campaign_code'] = aUtmCampaignCode[1]
                                continue
                        dictCode = self.__g_oSvCampaignParser.parse_campaign_code_fb(dictCampaignInfo)
                        if dictCode['detected'] == False:  # means bot finally does not find any standard info clearly
                            # ad creative without singleview standard url_tags nor alias_info found!
                            sCampaignName = dictCampaignInfo['ad_name']
                            dict_campaign_alias_rst = o_campaign_alias.get_detail_by_media_campaign_name(sCampaignName)
                            dictCode = dict_campaign_alias_rst['dict_ret']  # retrieve campaign name alias info
                            if dictCode['detected'] == False:
                                lst_non_sv_convention_campaign_title.append(sCampaignName)
            else:
                self._printDebug('pass ' + sDataFileFullname + ' does not exist')
            self._printProgressBar(nIdx + 1, nSentinel, prefix = 'validate data file:', suffix = 'Complete', length = 50)
            nIdx += 1
        del o_campaign_alias
        return list(set(lst_non_sv_convention_campaign_title))  # unique list

    def __arrange_fb_raw_data_file(self, sSvAcctId, s_brand_id):
        """ referring to raw_data_file, register raw data file """
        sDataPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, sSvAcctId, s_brand_id, 'fb_biz')
        # traverse directory and categorize data files
        lstTotalDataset = []
        lstFbBizAid = os.listdir(sDataPath)
        for sFbBizAid in lstFbBizAid:
            if sFbBizAid == 'alias_info_campaign.tsv':
                continue
            sDownloadDataPath = os.path.join(sDataPath, sFbBizAid, 'data')
            s_conf_path_abs = os.path.join(sDataPath, sFbBizAid, 'conf')
            sFxCode = self.__get_fx_code(s_conf_path_abs)
            self.__g_dictFxCodeByBizAcct[sFbBizAid] = sFxCode
            if sFxCode == 'error':
                self._printDebug('-> '+ sFbBizAid +' has been stopped')
                return

            if sFxCode != 'KRW':
                if self.__get_fx_trend(sFxCode) == False:
                    self._printDebug('-> '+ sFbBizAid +' has been stopped')

            self._printDebug('-> '+ sFbBizAid +' is analyzing FB IG data files')
            lstDataFiles = os.listdir(sDownloadDataPath)
            for sFilename in lstDataFiles:
                aFileExt = os.path.splitext(sFilename)
                if aFileExt[1] == '':
                    continue
                sDatafileTobeHandled = sFilename + '|@|' + sFbBizAid
                lstTotalDataset.append(sDatafileTobeHandled)
        
        lstTotalDataset.sort()
        o_campaign_alias = campaign_alias.CampaignAliasInfo(sSvAcctId, s_brand_id)
        nIdx = 0
        nSentinel = len(lstTotalDataset)
        for sFileInfo in lstTotalDataset:
            aFileInfo = sFileInfo.split('|@|')
            sFbBusinessAcctId = aFileInfo[1] 
            sTempDataPath = os.path.join(sDataPath, sFbBusinessAcctId, 'data')
            sDataFileFullname = os.path.join(sTempDataPath, aFileInfo[0])
            aFileDetailInfo = aFileInfo[0].split('_')
            sDatadate = aFileDetailInfo[0]
            if os.path.isfile(sDataFileFullname):
                with open(sDataFileFullname, 'r') as tsvfile:
                    reader = csv.reader(tsvfile, delimiter='\t')
                    for row in reader:
                        dictCampaignInfo = {'url_tags':row[5], 'ad_name':row[3], 'campaign_code':''}
                        lstUtmParams = row[5].split('&')
                        for sUtmParam in lstUtmParams:
                            if sUtmParam.find('utm_campaign=') > -1:
                                aUtmCampaignCode = sUtmParam.split('=')
                                dictCampaignInfo['campaign_code'] = aUtmCampaignCode[1]
                                continue
                        
                        dictCode = self.__g_oSvCampaignParser.parse_campaign_code_fb(dictCampaignInfo)
                        sUa = self.__g_oSvCampaignParser.get_ua(row[6])
                        if dictCode['detected'] == False:  # means bot finally does not find any standard info clearly
                            sCampaignName = dictCampaignInfo['ad_name']
                            dict_campaign_alias_rst = o_campaign_alias.get_detail_by_media_campaign_name(sCampaignName)
                            dictCode = dict_campaign_alias_rst['dict_ret']  # retrieve campaign name alias info
                            if dictCode['detected'] == False:
                                dictCode['source'] = self.__g_oSvCampaignParser.get_source_tag('FB')
                                dictCode['brd'] = 1
                                dictCode['medium'] = self.__g_oSvCampaignParser.get_sv_medium_tag('CPI')
                                dictCode['campaign1st'] = sCampaignName
                  
                        sReportId = sDatadate+'|@|' + sFbBusinessAcctId+'|@|' + sUa + '|@|'+dictCode['source']+'|@|' + \
                                    dictCode['rst_type'] + '|@|'+ dictCode['medium'] + '|@|' + str(dictCode['brd']) + '|@|'+\
                                    dictCode['campaign1st']+'|@|'+ dictCode['campaign2nd']+'|@|'+ dictCode['campaign3rd']
                        nReach = int(row[7])
                        nImpression = int(row[8])
                        nClick = int(row[9])
                        nUniqueClick = int(row[10])
                        sCurrentFxCode = self.__g_dictFxCodeByBizAcct[sFbBusinessAcctId]
                        if sCurrentFxCode == 'KRW':
                            nCost = float(row[11])
                        else:
                            nFxRate = self.__get_fx_rate(sCurrentFxCode, sDatadate)
                            nCost = float(row[11]) * nFxRate

                        try:
                            nConvCnt = float(row[13])
                        except IndexError:
                            nConvCnt = 0
                        try:
                            if sCurrentFxCode == 'KRW':
                                nConvAmnt = int(row[12])
                            else:
                                nConvAmnt = float(row[12]) * nFxRate
                        except IndexError:
                            nConvAmnt = 0
                    
                        if self.__g_dictFbRaw.get(sReportId, 0):  # returns 0 if sRowId does not exist
                            self.__g_dictFbRaw[sReportId]['reach'] += nReach
                            self.__g_dictFbRaw[sReportId]['imp'] += nImpression
                            self.__g_dictFbRaw[sReportId]['clk'] += nClick
                            self.__g_dictFbRaw[sReportId]['u_clk'] += nUniqueClick
                            self.__g_dictFbRaw[sReportId]['cost'] += nCost
                            self.__g_dictFbRaw[sReportId]['conv_cnt'] += nConvCnt
                            self.__g_dictFbRaw[sReportId]['conv_amnt'] += nConvAmnt
                        else:  # if new log requested
                            self.__g_dictFbRaw[sReportId] = {
                                'reach':nReach, 'imp':nImpression, 'u_clk':nUniqueClick, 'clk':nClick,
                                'cost':nCost, 'conv_cnt':nConvCnt, 'conv_amnt':nConvAmnt
                            }
                self.__archive_data_file(sTempDataPath, aFileInfo[0])
            else:
                self._printDebug('pass ' + sDataFileFullname + ' does not exist')
            self._printProgressBar(nIdx + 1, nSentinel, prefix = 'Arrange data file:', suffix = 'Complete', length = 50)
            nIdx += 1
        del o_campaign_alias

    def __register_db(self):
        nIdx = 0
        nSentinel = len(self.__g_dictFbRaw)
        with sv_mysql.SvMySql() as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            oSvMysql.set_app_name('svplugins.fb_register_db')
            oSvMysql.initialize(self._g_dictSvAcctInfo)
            for sReportId, dict_single_raw in self.__g_dictFbRaw.items():
                if not self._continue_iteration():
                    break
                aReportType = sReportId.split('|@|')
                sDataDate = datetime.strptime(aReportType[0], "%Y%m%d")
                sFbBizAcctId = aReportType[1]
                sUaType = aReportType[2]
                sSource = aReportType[3]
                sRstType = aReportType[4]
                sMedium = aReportType[5]
                bBrd = aReportType[6] 
                sCampaign1st = aReportType[7]
                sCampaign2nd = aReportType[8]
                sCampaign3rd = aReportType[9]
                # should check if there is duplicated date + SM log
                # strict str() formatting prevents that pymysql automatically rounding up tiny decimal
                oSvMysql.executeQuery('insertFbCompiledDailyLog', sFbBizAcctId, sUaType, sSource, sRstType, 
                    sMedium, bBrd, sCampaign1st, sCampaign2nd, sCampaign3rd,
                    str(dict_single_raw['cost']), str(dict_single_raw['reach']), dict_single_raw['imp'],
                    str(dict_single_raw['clk']), str(dict_single_raw['u_clk']), str(dict_single_raw['conv_cnt']),
                    str(dict_single_raw['conv_amnt']), sDataDate)

                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'Register DB:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __archive_data_file(self, sDataPath, sCurrentFileName):
        sSourcePath = sDataPath
        if not os.path.exists(sSourcePath):
            self._printDebug('error: fb data directory does not exist!')
            return
        
        sArchiveDataPath = os.path.join(sDataPath, 'archive')
        if not os.path.exists(sArchiveDataPath):
            os.makedirs(sArchiveDataPath)

        sSourceFilePath = os.path.join(sDataPath, sCurrentFileName)
        sArchiveDataFilePath = os.path.join(sArchiveDataPath, sCurrentFileName)
        shutil.move(sSourceFilePath, sArchiveDataFilePath)


if __name__ == '__main__': # for console debugging ex ) python3.6 task.py http://localhost/devel/svtest
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('fb_register_db')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
            pass
    else:
        print('warning! [config_loc] params are required for console execution.')
 