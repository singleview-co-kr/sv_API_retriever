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
import os
import sys
import configparser # https://docs.python.org/3/library/configparser.html

# sys.path.append('/usr/lib/python3.7/site-packages/facebook_business') # Replace this with the place you installed facebookads using pip
#sys.path.append('/opt/homebrew/lib/python2.7/site-packages/facebook_business-3.0.0-py2.7.egg-info') # same as above
import facebook_business
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative

# 3rd party library
from decouple import config 

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svdjango')
    import sv_object
    import sv_plugin
    import settings
    sys.path.append('../../conf') # singleview config
    # import fb_biz_config
else: # for platform running
    from svcommon import sv_object
    from svcommon import sv_plugin
    from django.conf import settings
    # singleview config
    # from conf import fb_biz_config


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_sFbApiVersion = 'v13.0'

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name+'(20221008)')

        # Declaring a dict outside of __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        self.__g_oConfig = configparser.ConfigParser()
        s_fb_biz_config_file = os.path.join(config('ABSOLUTE_PATH_BOT'), 'conf', 'fb_biz_config.ini')
        try:
            with open(s_fb_biz_config_file) as f:
                self.__g_oConfig.read_file(f)
                b_available = True
        except IOError:
            self._printDebug('slack_config.ini does not exist')
            # raise IOError('failed to initialize SvSlack')

        if b_available:
            self.__g_oConfig.read(s_fb_biz_config_file)
        
        self.__g_sCurrentFbBizAid = None
        self.__g_sAdAccountId = None
        self.__g_lstFields = None
        self.__g_sDownloadPath = None
        self.__g_lstAd = None
        self.__g_sLatestFilepath = None

    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        pass

    def do_task(self, o_callback):
        self._g_oCallback = o_callback

        dict_acct_info = self._task_pre_proc(o_callback)
        dict_acct_info = self._task_pre_proc(o_callback)
        if 'sv_account_id' not in dict_acct_info and 'brand_id' not in dict_acct_info:
            self._printDebug('stop -> invalid config_loc')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        if 'fb_biz_aid' not in dict_acct_info:
            self._printDebug('stop -> no fb business API info')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return

        s_sv_acct_id = dict_acct_info['sv_account_id']
        s_brand_id = dict_acct_info['brand_id']
        lst_fb_biz_aid = dict_acct_info['fb_biz_aid']
        if len(lst_fb_biz_aid) == 0:
            self._printDebug('stop -> no business account id')
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        try:
            for s_fb_biz_aid in lst_fb_biz_aid:
                self._printDebug('fb_get_day plugin launched with acct id ' + s_fb_biz_aid)
                self.__g_sCurrentFbBizAid = s_fb_biz_aid
                self.__getFbBusinessRaw(s_sv_acct_id, s_brand_id)
                self.__g_sCurrentFbBizAid = None
        except TypeError as error:
            # Handle errors in constructing a query.
            self._printDebug(('There was an error in constructing your query : %s' % error))
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        
        self._printDebug('fb_get_day plugin finished')
        self._task_post_proc(self._g_oCallback)

    def __getFbBusinessRaw(self, sSvAcctId, sAcctTitle):
        self.__g_sDownloadPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, sSvAcctId, sAcctTitle, 
            'fb_biz', self.__g_sCurrentFbBizAid, 'data')
        if os.path.isdir(self.__g_sDownloadPath) == False:
            os.makedirs(self.__g_sDownloadPath)
        s_conf_path_abs = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, sSvAcctId, sAcctTitle, 
            'fb_biz', self.__g_sCurrentFbBizAid, 'conf')
        if os.path.isdir(s_conf_path_abs) == False:
            os.makedirs(s_conf_path_abs)

        try:
            self.__g_sLatestFilepath = os.path.join(s_conf_path_abs, 'general.latest')
            f = open(self.__g_sLatestFilepath, 'r')
            sMaxReportDate = f.readline()
            dtStartRetrieval = datetime.strptime(sMaxReportDate, '%Y%m%d') + timedelta(days=1)
            f.close()
        except FileNotFoundError:
            dtStartRetrieval = datetime.now() - timedelta(days=1)

        #dtStartRetrieval = datetime(2018, 7, 31)
        self._printDebug('start date :'+dtStartRetrieval.strftime('%Y-%m-%d'))

        # requested report date should not be later than today
        dtDateEndRetrieval = datetime.now() - timedelta(days=1) # yesterday
        dtDateDiff = dtDateEndRetrieval - dtStartRetrieval
        nNumDays = int(dtDateDiff.days) + 1
        dictDateQueue = {}
        for x in range(0, nNumDays):
            dtElement = dtStartRetrieval + timedelta(days = x)
            dictDateQueue[dtElement] = 0
        
        if len(dictDateQueue) == 0:
            self._printDebug('no data to collect')
        
        b_period_compress_toggle = False
        if len(dictDateQueue) > 2:  # consider period compression
            dt_begin = list(dictDateQueue.keys())[0]
            dt_end = list(dictDateQueue.keys())[-1]
            n_yrmo_begin = int(dt_begin.strftime('%Y%m'))
            n_yrmo_end = int(dt_end.strftime('%Y%m'))
            del dt_begin, dt_end
            if n_yrmo_end > n_yrmo_begin:
                self._printDebug('toggle period compress mode')
                b_period_compress_toggle = True
        
        self.__g_sAdAccountId = 'act_' + self.__g_sCurrentFbBizAid
        FacebookAdsApi.init(access_token=self.__g_oConfig['COMMON']['ACCESS_TOKEN'], api_version=self.__g_sFbApiVersion)
        self.__g_lstAd = []
        oAccount = AdAccount(self.__g_sAdAccountId) #'your-adaccount-id'
        try:
            ads = oAccount.get_ads(fields=[
                Ad.Field.name,
                #Ad.Field.tracking_specs,
                Ad.Field.configured_status,
                Ad.Field.creative,])
        except facebook_business.exceptions.FacebookRequestError as err:
            if err.http_status() == 400 and err.get_message() == 'Call was not successful' and err.api_error_code() == 190:
                self._printDebug(err.api_error_message() + '\n' + \
                                'plz visit https://developers.facebook.com/apps/#app#id/marketing-api/tools/\n' + \
                                'token right select: ads_management, ads_read, read_insights -> get token\n' + \
                                'paste new token into /conf/fb_biz_config.ini')
            else:
                self._printDebug(err)
            return
        
        for oAds in ads:
            dictTempAd = {'id':oAds['id'], 'configured_status':oAds['configured_status'], 'creative_id':oAds['creative']['id'], 'name':oAds['name'] }
            #print('configured_status ' + oAds['configured_status'])
            #print('creative->creative_id ' + oAds['creative']['creative_id'])
            #print('creative->id ' + oAds['creative']['id'])
            #print('id ' + oAds['id'])
            #print('name '+ oAds['name'])
            self.__g_lstAd.append(dictTempAd)

        #oAccount = AdAccount(self.__g_sAdAccountId)
        oCreatives = oAccount.get_ad_creatives(fields=[
            AdCreative.Field.url_tags,
            AdCreative.Field.object_story_spec,
            #AdCreative.Field.name,
            #AdCreative.Field.image_hash,
            #AdCreative.Field.template_url,
            #AdCreative.Field.object_story_id,
        ])
        for dictCreatives in oCreatives:
            sCreativeId = dictCreatives['id']
            for dictAd in self.__g_lstAd:
                if dictAd['creative_id'] == sCreativeId:
                    sHostInfo = ''
                    try:
                        sUrlTag = dictCreatives['url_tags']
                    except KeyError:
                        sUrlTag = 'n/a'
                    
                    try:
                        for sStorySpec in dictCreatives['object_story_spec']:
                            if sStorySpec == 'link_data':
                                try:
                                    sHostInfo = dictCreatives['object_story_spec'][sStorySpec]['link']
                                except KeyError:
                                    sHostInfo = dictCreatives['object_story_spec'][sStorySpec]['child_attachments'][0]['link']
                            elif sStorySpec == 'video_data':
                                sHostInfo = dictCreatives['object_story_spec'][sStorySpec]['call_to_action']['value']['link']
                            else:
                                sHostInfo = dictCreatives['object_story_spec'][sStorySpec]
                    except KeyError:
                        sHostInfo = 'n/a'
                    
                    if sHostInfo.find('?') > -1:
                        aHostInfo = sHostInfo.split('?')
                        if aHostInfo[1].find('utm_campaign=') > -1:
                            try:
                                sHostInfo = aHostInfo[0]
                                sUrlTag = aHostInfo[1]
                            except IndexError:
                                pass

                    dictAd['url_tags'] = sUrlTag
                    dictAd['link'] = sHostInfo
                    continue
        # completed ad information list
        #sTsvFilename = dtStartRetrieval.strftime('%Y%m%d') + '_ad_creative.tsv'
        # write data stream to file.
        self.__g_lstFields = [
            'ad_id',
            'spend',
            'reach',
            'impressions',
            'clicks',
            'unique_clicks',
            'actions', # conversion count related
            'action_values', # conversion amount related
            #'frequency',
            #'relevance_score:score',
            #'actions:link_click',
            #'unique_actions:link_click',
            #'outbound_clicks:outbound_click',
        ]

        if b_period_compress_toggle:
            # collect period-compressed data
            lst_compressed_date = [list(dictDateQueue.keys())[0], list(dictDateQueue.keys())[-2]]
            self.__get_period_gross(lst_compressed_date)
            # collect last day daily data
            dt_yesterday = list(dictDateQueue.keys())[-1]
            del dictDateQueue
            dictDateQueue = {dt_yesterday: 0}
            self.__get_period_daily(dictDateQueue)
        else:  # collect regular daily data
            self.__get_period_daily(dictDateQueue)

    def __get_period_gross(self, lst_compressed_date):
        self.__get_fb_biz_data(lst_compressed_date)

    def __get_period_daily(self, dictDateQueue):
        while self._continue_iteration(): # loop for each report date
            try:
                dtRetrieval = list(dictDateQueue.keys())[list(dictDateQueue.values()).index(0)] # find unhandled report task
            except ValueError:
                break
    
            lst_compressed_date = [dtRetrieval, dtRetrieval]
            self.__get_fb_biz_data(lst_compressed_date)
            del lst_compressed_date
            dictDateQueue[dtRetrieval] = 1
            time.sleep(2)

    def __get_fb_biz_data(self, lst_compressed_date):
        s_datadate_begin = lst_compressed_date[0].strftime('%Y-%m-%d')
        s_datadate_end = lst_compressed_date[1].strftime('%Y-%m-%d')
        s_datadate_to_log = lst_compressed_date[1].strftime('%Y%m%d')
        self._printDebug( '--> '+ self.__g_sCurrentFbBizAid +' will retrieve report from ' + s_datadate_begin + ' to ' + s_datadate_end)
        params = {
            'level': 'ad',
            #'filtering': [],
            'breakdowns': ['device_platform',],  #['gender','age'],
            'time_range': {'since':s_datadate_begin,'until':s_datadate_end}, # 'date_preset': Insights.Preset.yesterday,
        }
        oInsights = AdAccount(self.__g_sAdAccountId).get_insights(
            fields=self.__g_lstFields,
            params=params,
        )
        try:
            if len(oInsights) > 0:
                sTsvFilename = s_datadate_to_log + '_general.tsv'
                with open(self.__g_sDownloadPath+'/'+sTsvFilename, 'w', encoding='utf-8') as out:
                    for dictInsight in oInsights:
                        nConversionValue = 0  
                        nConversionCount = 0
                        try:
                            for dictActionVals in dictInsight['action_values']:
                                if dictActionVals['action_type'] == 'offsite_conversion.fb_pixel_purchase' or dictActionVals['action_type'] == 'offsite_conversion.fb_pixel_view_content':
                                    nConversionValue = dictActionVals['value']
                            
                            for dictActionVals in dictInsight['actions']:
                                if dictActionVals['action_type'] == 'offsite_conversion.fb_pixel_purchase' or dictActionVals['action_type'] == 'offsite_conversion.fb_pixel_view_content':
                                    nConversionCount = dictActionVals['value']
                        except KeyError:
                            pass

                        sAdIdFromInsight = dictInsight['ad_id']
                        for dictAd in self.__g_lstAd:
                            if dictAd['id'] == sAdIdFromInsight:
                                if 'unique_clicks' in dictInsight:
                                    nUniqueClicks = dictInsight['unique_clicks']
                                else:
                                    nUniqueClicks = 0

                                if 'spend' in dictInsight:
                                    nSpend = dictInsight['spend']
                                else:
                                    nSpend = 0

                                if 'reach' in dictInsight:
                                    n_reach = dictInsight['reach']
                                else:
                                    n_reach = 0

                                if 'impressions' in dictInsight:
                                    n_impressions = dictInsight['impressions']
                                else:
                                    n_impressions = 0

                                if 'clicks' in dictInsight:
                                    n_clicks = dictInsight['clicks']
                                else:
                                    n_clicks = 0

                                # write data stream to file.
                                sRow = dictAd['id'] + '\t' + dictAd['configured_status'] + '\t' + dictAd['creative_id'] + '\t' + dictAd['name'] + '\t' + dictAd['link'] + '\t' + dictAd['url_tags'] + '\t' + \
                                        dictInsight['device_platform'] + '\t' + str(n_reach) + '\t' + str(n_impressions) + '\t' + str(n_clicks) + '\t' + \
                                        str(nUniqueClicks) + '\t' + str(nSpend) + '\t' + str(nConversionValue) + '\t' + str(nConversionCount) + '\n'
                                out.write(sRow)
                            continue
            else:
                self._printDebug('WARNING! no data detected!\nstop querying if you do not spend.\nfb api does not like free querying\nthey might block API access temporarily.')
            
            try:
                f = open(self.__g_sLatestFilepath, 'w')
                f.write(s_datadate_to_log)
                f.close()
            except PermissionError:
                self._printDebug('write permission error')
                self._printDebug(e)
                
            time.sleep(2)
        except Exception as e:
            self._printDebug('unknown exception occured')
            self._printDebug(e)


if __name__ == '__main__': # for console debugging and execution
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('fb_get_day')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
            pass
    else:
        print('warning! [config_loc] params are required for console execution.')
 