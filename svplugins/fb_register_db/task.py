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
    import sv_mysql
    import sv_campaign_parser
    import sv_object, sv_plugin
    sys.path.append('../../conf') # singleview config
    import basic_config
else: # for platform running
    from svcommon import sv_mysql
    from svcommon import sv_campaign_parser
    from svcommon import sv_object, sv_plugin
    # singleview config
    from conf import basic_config # singleview config


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_oSvCampaignParser = None
    __g_sTblPrefix = None
    __g_dictFbRaw = None  # prevent duplication on a web console
    __g_dictFxCodeByBizAcct = {}
    __g_dictFxTrendInfo = {}

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        self._g_sVersion = '1.0.1'
        self._g_sLastModifiedDate = '12th, Oct 2021'
        self._g_oLogger = logging.getLogger(__name__ + ' v'+self._g_sVersion)

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        pass

    def do_task(self, o_callback):
        self.__g_dictFbRaw = {}  # prevent duplication on a web console

        oResp = self._task_pre_proc(o_callback)
        dict_acct_info = oResp['variables']['acct_info']
        if dict_acct_info is None:
            self._printDebug('stop -> invalid config_loc')
            return

        self.__g_oSvCampaignParser = sv_campaign_parser.svCampaignParser()        
        s_sv_acct_id = list(dict_acct_info.keys())[0]
        s_acct_title = dict_acct_info[s_sv_acct_id]['account_title']
        self.__g_sTblPrefix = dict_acct_info[s_sv_acct_id]['tbl_prefix']
        
        with sv_mysql.SvMySql('svplugins.fb_register_db') as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.initialize()
        
        self.__arrangeFbRawDataFile(s_sv_acct_id, s_acct_title)
        self.__registerDb()

        self._task_post_proc(o_callback)

    def __getFxRate(self, sCheckFxCode, sCheckDate):
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

    def __getFxTrend(self, sFxCode):
        # https://developers.facebook.com/docs/marketing-api/currencies/
        if sFxCode == 'KRW':
            return True
        
        sCurrencyTrendPath = os.path.join(basic_config.ABSOLUTE_PATH_BOT, 'files', 'info_fx_' + sFxCode + '.tsv')
        try:
            with open(sCurrencyTrendPath, 'r') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                nRowCnt = 0
                for row in reader:
                    if nRowCnt > 0: 
                        self.__g_dictFxTrendInfo[sFxCode + '_' + row[0]] = int(row[1].replace(',',''))
                    nRowCnt = nRowCnt + 1
        except FileNotFoundError:
            self._printDebug('file ' + sCurrencyTrendPath + ' does not exist')
            return False
        return True

    def __getFxCode(self, s_conf_path_abs):
        # https://developers.facebook.com/docs/marketing-api/currencies/
        sFxCodePath = os.path.join(s_conf_path_abs, 'info_fx.tsv')
        sFxCode = 'error'
        try:
            with open(sFxCodePath, 'r') as f:
                sFxCode = f.readline().strip().upper()
        except FileNotFoundError:
            self._printDebug('file ' + sFxCodePath + ' does not exist')
        return sFxCode

    def __arrangeFbRawDataFile(self, sSvAcctId, sAcctTitle):
        sDataPath = os.path.join(basic_config.ABSOLUTE_PATH_BOT, 'files', sSvAcctId, sAcctTitle, 'fb_biz')
        # traverse directory and categorize data files
        lstTotalDataset = []
        lstFbBizAid = os.listdir(sDataPath)
        for sFbBizAid in lstFbBizAid:
            if sFbBizAid == 'alias_info_campaign.tsv':
                continue

            sDownloadDataPath = os.path.join(sDataPath, sFbBizAid, 'data')
            s_conf_path_abs = os.path.join(sDataPath, sFbBizAid, 'conf')
            sFxCode = self.__getFxCode(s_conf_path_abs)
            self.__g_dictFxCodeByBizAcct[sFbBizAid] = sFxCode
            if sFxCode == 'error':
                self._printDebug('-> '+ sFbBizAid +' has been stopped')
                return

            if sFxCode != 'KRW':
                if self.__getFxTrend(sFxCode) == False:
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
        dictCampaignNameAlias = self.__getCampaignNameAlias(sSvAcctId, sAcctTitle)
        nIdx = 0
        nSentinel = len(lstTotalDataset)
        for sFileInfo in lstTotalDataset:
            aFileInfo = sFileInfo.split('|@|')
            sFbBusinessAcctId = aFileInfo[1] 
            sTempDataPath = os.path.join(sDataPath, sFbBusinessAcctId, 'data')
            sDataFileFullname = os.path.join(sTempDataPath, aFileInfo[0])
            aFileDetailInfo = aFileInfo[0].split('_')
            sDatadate = aFileDetailInfo[0]
            try:
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
                        
                        dictCode = self.__g_oSvCampaignParser.parseCampaignCodeFb(dictCampaignInfo, dictCampaignNameAlias)
                        sUa = self.__g_oSvCampaignParser.getUa(row[6])
                        if dictCode['source'] == 'unknown':  # for debugging
                            self._printDebug(row)

                        if dictCode['detected'] == False:  # means bot finally does not find any standard info clearly
                            self._printDebug('ad creative without singleview standard url_tags nor alias_info found!')
                            self._printDebug(row)
                            self._printDebug(dictCode)
                        
                        sReportId = sDatadate+'|@|'+sFbBusinessAcctId+'|@|'+sUa+'|@|'+dictCode['source']+'|@|'+dictCode['rst_type']+'|@|'+ dictCode['medium']+'|@|'+\
                            dictCode['brd']+'|@|'+\
                            dictCode['campaign1st']+'|@|'+\
                            dictCode['campaign2nd']+'|@|'+\
                            dictCode['campaign3rd']
                    
                        nReach = int(row[7])
                        nImpression = int(row[8])
                        nClick = int(row[9])
                        nUniqueClick = int(row[10])
                        
                        sCurrentFxCode = self.__g_dictFxCodeByBizAcct[sFbBusinessAcctId]
                        if sCurrentFxCode == 'KRW':
                            nCost = float(row[11])
                        else:
                            nFxRate = self.__getFxRate(sCurrentFxCode, sDatadate)
                            nCost = float(row[11]) * nFxRate

                        try:
                            nConvCnt = float(row[12])
                        except IndexError:
                            nConvCnt = 0
                        try:
                            if sCurrentFxCode == 'KRW':
                                nConvAmnt = int(row[13])
                            else:
                                nConvAmnt = float(row[13]) * nFxRate
                        except IndexError:
                            nConvAmnt = 0
                    
                        try: # if designated log already created
                            self.__g_dictFbRaw[sReportId]
                            self.__g_dictFbRaw[sReportId]['reach'] += nReach
                            self.__g_dictFbRaw[sReportId]['imp'] += nImpression
                            self.__g_dictFbRaw[sReportId]['clk'] += nClick
                            self.__g_dictFbRaw[sReportId]['u_clk'] += nUniqueClick
                            self.__g_dictFbRaw[sReportId]['cost'] += nCost
                            self.__g_dictFbRaw[sReportId]['conv_cnt'] += nConvCnt
                            self.__g_dictFbRaw[sReportId]['conv_amnt'] += nConvAmnt
                        except KeyError: # if new log requested
                            self.__g_dictFbRaw[sReportId] = {
                                'reach':nReach,'imp':nImpression,'u_clk':nUniqueClick,'clk':nClick,'cost':nCost,'conv_cnt':nConvCnt,'conv_amnt':nConvAmnt
                            }
                self.__archiveGaDataFile(sTempDataPath, aFileInfo[0])
            except FileNotFoundError:
                self._printDebug('pass ' + sDataFileFullname + ' does not exist')

            self._printProgressBar(nIdx + 1, nSentinel, prefix = 'Arrange data file:', suffix = 'Complete', length = 50)
            nIdx += 1

    def __registerDb(self):
        nIdx = 0
        nSentinel = len(self.__g_dictFbRaw)
        with sv_mysql.SvMySql('svplugins.fb_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            for sReportId in self.__g_dictFbRaw:
                if not self._continue_iteration():
                    break

                aReportType = sReportId.split('|@|')
                sDataDate = datetime.strptime( aReportType[0], "%Y%m%d" )
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
                lstFetchRst = oSvMysql.executeQuery('insertFbCompiledDailyLog', sFbBizAcctId, sUaType, sSource, sRstType, sMedium, bBrd, sCampaign1st, sCampaign2nd, sCampaign3rd,
                    str(self.__g_dictFbRaw[sReportId]['cost']), str(self.__g_dictFbRaw[sReportId]['reach']), self.__g_dictFbRaw[sReportId]['imp'], str(self.__g_dictFbRaw[sReportId]['clk']),
                    str(self.__g_dictFbRaw[sReportId]['u_clk']),str(self.__g_dictFbRaw[sReportId]['conv_cnt']), str(self.__g_dictFbRaw[sReportId]['conv_amnt']), sDataDate )

                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'Register DB:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __getCampaignNameAlias(self, sSvAcctId, sAcctTitle):
        sParentDataPath = os.path.join(basic_config.ABSOLUTE_PATH_BOT, 'files', sSvAcctId, sAcctTitle, 'fb_biz')
        dictCampaignNameAliasInfo = {}
        try:
            with codecs.open(os.path.join(sParentDataPath, 'alias_info_campaign.tsv'), 'r',encoding='utf8') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                nRowCnt = 0
                for row in reader:
                    if nRowCnt > 0:
                        dictCampaignNameAliasInfo[row[0]] = {'source':row[1], 'rst_type':row[2], 'medium':row[3], 'camp1st':row[4], 'camp2nd':row[5], 'camp3rd':row[6] }

                    nRowCnt = nRowCnt + 1
        except FileNotFoundError:
            pass
        return dictCampaignNameAliasInfo

    def __archiveGaDataFile(self, sDataPath, sCurrentFileName):
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
        print('warning! [analytical_namespace] [config_loc] params are required for console execution.')
 