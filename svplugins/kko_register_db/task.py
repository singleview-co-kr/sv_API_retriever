# -*- coding: UTF-8 -*-

# Copyright 2014 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# standard library
import logging
from datetime import datetime
import os
import shutil
import csv
import codecs
import sys

if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svdjango')
    import sv_mysql
    import sv_campaign_parser
    import sv_object
    import sv_plugin
    import settings
else:
    from svcommon import sv_mysql
    from svcommon import sv_campaign_parser
    from svcommon import sv_object
    from svcommon import sv_plugin
    from django.conf import settings


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_oSvCampaignParser = sv_campaign_parser.svCampaignParser()

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        self._g_sLastModifiedDate = '25th, Jan 2022'
        self._g_oLogger = logging.getLogger(__name__ + ' modified at '+self._g_sLastModifiedDate)
        # Declaring a dict outside of __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        self.__g_dictKkoRaw = {}

    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        self.__g_dictKkoRaw = {}

    def do_task(self, o_callback):
        self._g_oCallback = o_callback

        oResp = self._task_pre_proc(o_callback)
        dict_acct_info = oResp['variables']['acct_info']
        if dict_acct_info is None:
            self._printDebug('stop -> invalid config_loc')
            self._task_post_proc(self._g_oCallback)
            return

        s_sv_acct_id = list(dict_acct_info.keys())[0]
        s_acct_title = dict_acct_info[s_sv_acct_id]['account_title']
        s_kakao_acct_id = dict_acct_info[s_sv_acct_id]['kko_moment_aid']
        self.__g_sTblPrefix = dict_acct_info[s_sv_acct_id]['tbl_prefix']
        with sv_mysql.SvMySql('svplugins.kko_register_db') as oSvMysql:
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            oSvMysql.initialize() 
        
        self._printDebug('-> register kko raw data')
        self.__arrangeKkoRawDataFile(s_sv_acct_id, s_acct_title, s_kakao_acct_id)
        self.__registerDb()

        self._task_post_proc(self._g_oCallback)

    def __getCampaignNameAlias(self, sParentDataPath):
        dictCampaignNameAliasInfo = {}
        s_alias_filepath = os.path.join(sParentDataPath, 'alias_info_campaign.tsv')
        if os.path.isfile(s_alias_filepath):
            with codecs.open(s_alias_filepath, 'r',encoding='utf8') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                nRowCnt = 0
                for row in reader:
                    if nRowCnt > 0:
                        sCampaignUniqueKey = row[0] + '|@|' + row[1] + '|@|' + row[2]
                        dictCampaignNameAliasInfo[sCampaignUniqueKey] = {'source':row[3], 'rst_type':row[4], 'medium':row[5], 'camp1st':row[6], 'camp2nd':row[7], 'camp3rd':row[8] }
                    nRowCnt = nRowCnt + 1
        return dictCampaignNameAliasInfo

    def __arrangeKkoRawDataFile(self, sSvAcctId, sAcctTitle, sKakaoAcctId):
        lstMergedDataFiles = []
        sParentDataPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, sSvAcctId, sAcctTitle, 'kakao')
        # retrieve campaign name alias info
        dictCampaignNameAlias = self.__getCampaignNameAlias(sParentDataPath)
        sDataPath = os.path.join(sParentDataPath, sKakaoAcctId, 'data')
        # traverse directory and categorize data files
        lstDataFiles = os.listdir(sDataPath)
        for nIdx, sDatafileName in enumerate(lstDataFiles):
            aFileExt = os.path.splitext(sDatafileName)
            if aFileExt[0] == 'agency_info' or aFileExt[0] == 'archive':
                continue
            sMode = 'r' # means regular daily
            lstMergedDataFiles.append(sDatafileName + '|@|' + sKakaoAcctId + '|@|' + sMode)

        lstMergedDataFiles.sort()
        nIdx = 0
        nSentinel = len(lstMergedDataFiles)
        for sDataFileInfo in lstMergedDataFiles:
            aDataFileInfo = sDataFileInfo.split('|@|')
            sFilename = aDataFileInfo[0]
            sCid = aDataFileInfo[1]
            sMode = aDataFileInfo[2]
            if sMode == 'r':
                sDataFileFullname = os.path.join(sDataPath, sFilename)
            if int(sFilename.split('.')[0]) < 20200420:
                self.__procKkoRawDataFileV1(sCid, sDataPath, sDataFileFullname, sFilename, dictCampaignNameAlias)
            else:
                self.__procKkoRawDataFileV2(sCid, sDataPath, sDataFileFullname, sFilename, dictCampaignNameAlias)
            
            self._printProgressBar(nIdx + 1, nSentinel, prefix = 'Arrange data file:', suffix = 'Complete', length = 50)
            nIdx += 1

    def __procKkoRawDataFileV2(self, sCid, sSourceDataPath, sDataFileFullname, sFilename, dictCampaignNameAlias):
        self._printDebug('v2 format')
        # 유형x목표	row[0]
        # 캠페인 row[1]
        # 광고그룹 row[2]
        # 소재 row[3]
        # 시작일 row[4]
        # 종료일 row[5]
        # 디바이스 row[6]
        # 비용 row[7]
        # 노출수 row[8]
        # 클릭수 row[9]
        # 클릭률 row[10]
        # 도달수 row[11]
        # 노출당 비용 row[12]
        # 클릭당 비용 row[13]
        # 도달당 비용 row[14]
        # 가입완료(직접) row[15]
        # 장바구니 보기(직접) row[16]
        # 구매(직접) row[17]
        # 구매금액(직접) row[18]
        # 가입완료(간접) row[19]
        # 장바구니 보기(간접) row[20]
        # 구매(간접) row[21]
        # 구매금액(간접) row[22]
        if not os.path.isfile(sDataFileFullname):
            self._printDebug('pass ' + sDataFileFullname + ' does not exist')
            return

        with open(sDataFileFullname, 'r') as tsvfile:
            nRowCnt = 0
            reader = csv.reader(tsvfile, delimiter='\t')
            for row in reader:
                if nRowCnt == 0: # ignore TSV file header
                    nRowCnt = nRowCnt + 1
                    continue
                nRowCnt = nRowCnt + 1
                if int(row[8]) == 0: # if impression is zero
                    continue
                
                bBrd = 0
                sSource = None
                sRstType = None
                sMedium = None
                sCampaign1st = None
                sCampaign2nd = None
                sCampaign3rd = None
                lstCampaignCode = row[3].split('_')
                # process body
                if len(lstCampaignCode) == 6: # adwords campaign name follows singleview campaign code
                    if lstCampaignCode[0] == 'KKO':
                        sSource = lstCampaignCode[0]
                        sRstType = lstCampaignCode[1]
                        sMedium = lstCampaignCode[2]
                        sCampaign1st = lstCampaignCode[3]
                        sCampaign2nd = lstCampaignCode[4]
                        sCampaign3rd = lstCampaignCode[5]
                
                # if non singleview standard ad name
                if sSource == None or sRstType == None or sMedium == None or sCampaign1st == None or \
                    sCampaign2nd == None or sCampaign3rd == None:
                    sCampaignName = row[1] + '|@|' + row[2] + '|@|' + row[3]
                    if dictCampaignNameAlias.get(sCampaignName, 0):  # returns 0 if sRowId does not exist
                        sSource = dictCampaignNameAlias[sCampaignName]['source']
                        sRstType = dictCampaignNameAlias[sCampaignName]['rst_type']
                        sMedium = dictCampaignNameAlias[sCampaignName]['medium']
                        sCampaign1st = dictCampaignNameAlias[sCampaignName]['camp1st']
                        sCampaign2nd = dictCampaignNameAlias[sCampaignName ]['camp2nd']
                        sCampaign3rd = dictCampaignNameAlias[sCampaignName]['camp3rd']
                    else:  # if unacceptable googleads campaign name
                        self._printDebug('  ' + sCampaignName + '  ' + sDataFileFullname)
                        self._printDebug('weird kakao moment log!')
                        return
                
                sUa = self.__g_oSvCampaignParser.getUa(row[6])
                nImpression = int(row[8])
                nClick = int(row[9])
                nCost = int(row[7])
                nConvCntDirect = int(float(row[17]))
                nConvAmntDirect = int(float(row[18]))
                nConvCntIndirect = int(float(row[21]))
                nConvAmntIndirect = int(float(row[22]))
                sDatadate = row[4]
                sReportId = sCid+'|@|'+sDatadate+'|@|'+sUa+'|@|'+sSource+'|@|'+sRstType+'|@|'+ sMedium+'|@|'+ str(bBrd)+'|@|'+\
                    sCampaign1st+'|@|'+	sCampaign2nd+'|@|'+	sCampaign3rd # +'|@|'+ str(sTerm)

                if sReportId in self.__g_dictKkoRaw.keys():  # if designated log already created
                    self.__g_dictKkoRaw[sReportId]['imp'] += nImpression
                    self.__g_dictKkoRaw[sReportId]['clk'] += nClick
                    self.__g_dictKkoRaw[sReportId]['cost_inc_vat'] += nCost
                    self.__g_dictKkoRaw[sReportId]['conv_cnt_direct'] += nConvCntDirect
                    self.__g_dictKkoRaw[sReportId]['conv_amnt_direct'] += nConvAmntDirect
                    self.__g_dictKkoRaw[sReportId]['conv_cnt_indirect'] += nConvCntIndirect
                    self.__g_dictKkoRaw[sReportId]['conv_amnt_indirect'] += nConvAmntIndirect
                else:  # if new log requested
                    self.__g_dictKkoRaw[sReportId] = {
                        'imp':nImpression, 'clk':nClick, 'cost_inc_vat':nCost, 'conv_cnt_direct':nConvCntDirect,
                        'conv_amnt_direct':nConvAmntDirect, 'conv_cnt_indirect':nConvCntIndirect,
                        'conv_amnt_indirect':nConvAmntIndirect
                    }
        self.__archiveGaDataFile(sSourceDataPath, sFilename)

    def __procKkoRawDataFileV1(self, sCid, sSourceDataPath, sDataFileFullname, sFilename, dictCampaignNameAlias):
        self._printDebug( 'v1 format')
        # 디스플레이 캠페인 row[0]
        # 광고그룹 row[1]
        # 소재 row[2]
        # 시작일 row[3]
        # 종료일 row[4]
        # 디바이스 row[5]
        # 노출수 row[6]
        # 클릭수 row[7]
        # 클릭률 row[8]
        # 비용 row[9]
        # 가입완료(직접) row[10]
        # 장바구니 보기(직접) row[11]
        # 구매(직접) row[12]
        # 구매금액(직접) row[13]
        # 가입완료(간접) row[14]
        # 장바구니 보기(간접) row[15]
        # 구매(간접) row[16]
        # 구매금액(간접) row[17]
        if not os.path.isfile(sDataFileFullname):
            self._printDebug('pass ' + sDataFileFullname + ' does not exist')
            return

        with open(sDataFileFullname, 'r') as tsvfile:
            nRowCnt = 0
            reader = csv.reader(tsvfile, delimiter='\t')
            for row in reader:
                if nRowCnt == 0: # ignore TSV file header
                    nRowCnt = nRowCnt + 1
                    continue
                    
                nRowCnt = nRowCnt + 1
                if int(row[6]) == 0: # if impression is zero
                    continue
                
                bBrd = 0
                sSource = None
                sRstType = None
                sMedium = None
                sCampaign1st = None
                sCampaign2nd = None
                sCampaign3rd = None
                lstCampaignCode = row[2].split('_')
                # process body
                if len(lstCampaignCode) == 6: # adwords campaign name follows singleview campaign code
                    if lstCampaignCode[0] == 'KKO':
                        sSource = lstCampaignCode[0]
                        sRstType = lstCampaignCode[1]
                        sMedium = lstCampaignCode[2]
                        sCampaign1st = lstCampaignCode[3]
                        sCampaign2nd = lstCampaignCode[4]
                        sCampaign3rd = lstCampaignCode[5]
                # if non singleview standard ad name
                if sSource == None or sRstType == None or sMedium == None or sCampaign1st == None or \
                    sCampaign2nd == None or sCampaign3rd == None:
                    sCampaignName = row[0] + '|@|' + row[1] + '|@|' + row[2]
                    if dictCampaignNameAlias.get(sCampaignName, 0):  # returns 0 if sRowId does not exist
                        sSource = dictCampaignNameAlias[sCampaignName]['source']
                        sRstType = dictCampaignNameAlias[sCampaignName]['rst_type']
                        sMedium = dictCampaignNameAlias[sCampaignName]['medium']
                        sCampaign1st = dictCampaignNameAlias[sCampaignName]['camp1st']
                        sCampaign2nd = dictCampaignNameAlias[sCampaignName]['camp2nd']
                        sCampaign3rd = dictCampaignNameAlias[sCampaignName]['camp3rd']
                    else:  # if unacceptable googleads campaign name
                        self._printDebug('  ' + sCampaignName + '  ' + sDataFileFullname)
                        self._printDebug('weird kakao moment log!')
                        return
                sUa = self.__g_oSvCampaignParser.getUa(row[5])
                nImpression = int(row[6])
                nClick = int(row[7])
                nCost = int(row[9])
                nConvCntDirect = int(float(row[12]))
                nConvAmntDirect = int(float(row[13]))
                nConvCntIndirect = int(float(row[16]))
                nConvAmntIndirect = int(float(row[17]))
                sDatadate = row[3]
                sReportId = sCid+'|@|'+sDatadate+'|@|'+sUa+'|@|'+sSource+'|@|'+sRstType+'|@|'+ sMedium+'|@|'+ str(bBrd)+'|@|'+\
                    sCampaign1st+'|@|'+	sCampaign2nd+'|@|'+	sCampaign3rd # +'|@|'+ str(sTerm)

                if sReportId in self.__g_dictKkoRaw.keys():
                    self.__g_dictKkoRaw[sReportId]['imp'] += nImpression
                    self.__g_dictKkoRaw[sReportId]['clk'] += nClick
                    self.__g_dictKkoRaw[sReportId]['cost_inc_vat'] += nCost
                    self.__g_dictKkoRaw[sReportId]['conv_cnt_direct'] += nConvCntDirect
                    self.__g_dictKkoRaw[sReportId]['conv_amnt_direct'] += nConvAmntDirect
                    self.__g_dictKkoRaw[sReportId]['conv_cnt_indirect'] += nConvCntIndirect
                    self.__g_dictKkoRaw[sReportId]['conv_amnt_indirect'] += nConvAmntIndirect
                else:
                    self.__g_dictKkoRaw[sReportId] = {
                        'imp':nImpression,'clk':nClick, 'cost_inc_vat':nCost, 'conv_cnt_direct':nConvCntDirect,
                        'conv_amnt_direct':nConvAmntDirect, 'conv_cnt_indirect':nConvCntIndirect, 'conv_amnt_indirect':nConvAmntIndirect
                    }
        self.__archiveGaDataFile(sSourceDataPath, sFilename)

    def __registerDb(self):
        nIdx = 0
        nSentinel = len(self.__g_dictKkoRaw)
        with sv_mysql.SvMySql('svplugins.kko_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            for sReportId, dict_single_row in self.__g_dictKkoRaw.items():
                aReportType = sReportId.split('|@|')
                sKkoCid = aReportType[0]
                sDataDate = datetime.strptime(aReportType[1], "%Y-%m-%d")
                sUaType = aReportType[2]
                sSource = aReportType[3]
                sRstType = aReportType[4]
                sMedium = self.__g_oSvCampaignParser.getSvMediumTag(aReportType[5])
                bBrd = aReportType[6]
                sCampaign1st = aReportType[7]
                sCampaign2nd = aReportType[8]
                sCampaign3rd = aReportType[9]
                sTerm = '' #aReportType[10]
                # should check if there is duplicated date + SM log
                # strict str() formatting prevents that pymysql automatically rounding up tiny decimal
                oSvMysql.executeQuery('insertKkoCompiledDailyLog', sKkoCid, sUaType, sSource, sRstType, sMedium, 
                    bBrd, sCampaign1st, sCampaign2nd, sCampaign3rd,sTerm,
                    str(dict_single_row['cost_inc_vat']), dict_single_row['imp'], str(dict_single_row['clk']),
                    str(dict_single_row['conv_cnt_direct']), str(dict_single_row['conv_amnt_direct']),
                    str(dict_single_row['conv_cnt_indirect']), str(dict_single_row['conv_amnt_indirect']), sDataDate )
                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'Register DB:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __archiveGaDataFile(self, sSourcePath, sCurrentFileName):
        if not os.path.exists(sSourcePath):
            self._printDebug('error: adw source directory does not exist!')
            return
        sArchiveDataPath = os.path.join(sSourcePath, 'archive')
        if not os.path.exists(sArchiveDataPath):
            os.makedirs(sArchiveDataPath)
        sSourceFilePath = os.path.join(sSourcePath, sCurrentFileName)
        sArchiveDataFilePath = os.path.join(sArchiveDataPath, sCurrentFileName)
        shutil.move(sSourceFilePath, sArchiveDataFilePath)


if __name__ == '__main__': # for console debugging
    # python task.py analytical_namespace=test config_loc=1/ynox
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('kko_register_db')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [analytical_namespace] [config_loc] params are required for console execution.')
