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

# references for more information
# https://stackoverflow.com/questions/50266800/object-with-id-203618703567212-does-not-exist-cannot-be-loaded-due-to-missing
# https://stackoverflow.com/questions/8231877/facebook-access-token-for-pages
# https://developers.facebook.com/docs/marketing-api/reference/sdks/python/ad-account/v3.1
# https://developers.facebook.com/docs/marketing-api/insights
# https://developers.facebook.com/docs/marketing-api/reference/ad-creative
# https://developers.facebook.com/docs/marketing-api/access/
# https://developers.facebook.com/docs/apps/review/server-to-server-apps/
# https://developers.facebook.com/apps/472350069614767/marketing-api/quickstart/

# standard library
import logging
from datetime import datetime
from datetime import timedelta
import os
import sys
import csv
import configparser # https://docs.python.org/3/library/configparser.html

# sys.path.append('/usr/lib/python3.6/site-packages/facebook_business') # Replace this with the place you installed facebookads using pip
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
else: # for platform running
    from svcommon import sv_object
    from svcommon import sv_plugin
    # singleview config
    from django.conf import settings


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_sFbApiVersion = 'v13.0'

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        self._g_oLogger = logging.getLogger(__name__ + ' modified at 8th, May 2022')
        self._g_dictParam.update({'earliest_date':None, 'latest_date':None})
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

        if b_available:
            self.__g_oConfig.read(s_fb_biz_config_file)

        self.__g_sDataFirstDate = None
        self.__g_sDataLastDate = None

    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        self.__g_sDataFirstDate = None
        self.__g_sDataLastDate = None

    def do_task(self, o_callback):
        self._g_oCallback = o_callback
        
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

        if self._g_dictParam['earliest_date'] is None or \
            self._g_dictParam['latest_date'] is None:
            self._printDebug('you should designate earliest_date and latest_date')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        self.__g_sDataLastDate = self._g_dictParam['latest_date'].replace('-','')
        self.__g_sDataFirstDate = self._g_dictParam['earliest_date'].replace('-','')

        s_sv_acct_id = dict_acct_info['sv_account_id']
        s_brand_id = dict_acct_info['brand_id']
        s_fb_biz_aid = dict_acct_info['fb_biz_aid']
        if s_fb_biz_aid == '':
            self._printDebug('stop -> no business account id')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        
        self._printDebug('fb_get_day plugin launched with acct id ' + s_fb_biz_aid)
        try:
            self.__getFbBusinessRaw(s_sv_acct_id, s_brand_id, s_fb_biz_aid)
        except TypeError as error:
            # Handle errors in constructing a query.
            self._printDebug(('There was an error in constructing your query : %s' % error))
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return

        self._task_post_proc(self._g_oCallback)
        
    def __getFbBusinessRaw(self, sSvAcctId, sAcctTitle, sFbBizAid):
        sDownloadPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, sSvAcctId, sAcctTitle, 'fb_biz', sFbBizAid, 'data')
        if os.path.isdir(sDownloadPath) == False:
            os.makedirs(sDownloadPath)
        s_conf_path_abs = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, sSvAcctId, sAcctTitle, 'fb_biz', sFbBizAid, 'conf')
        if os.path.isdir(s_conf_path_abs) == False:
            os.makedirs(s_conf_path_abs)

        try:
            sEarliestFilepath = os.path.join(s_conf_path_abs, 'general.earliest')
            f = open(sEarliestFilepath, 'r')
            sMinReportDate = f.readline()
            dtDateDataRetrieval = datetime.strptime(sMinReportDate, '%Y%m%d') - timedelta(days=1)
            f.close()
        except FileNotFoundError:
            dtDateDataRetrieval = datetime.strptime(self.__g_sDataLastDate, '%Y%m%d')
        
        # if requested date is earlier than first date
        if dtDateDataRetrieval - datetime.strptime(self.__g_sDataFirstDate, '%Y%m%d') < timedelta(days=0): 
            s_msg = 'meet earliest stat date -> remove the job and toggle the job table'
            if self._g_bDaemonEnv:  # for running on dbs.py only
                logging.info(s_msg)
                raise Exception('completed')
            else:
                self._printDebug(s_msg)
                return

        sAdAccountId = 'act_'+sFbBizAid
        FacebookAdsApi.init(access_token=self.__g_oConfig['COMMON']['ACCESS_TOKEN'], api_version=self.__g_sFbApiVersion)
        lstAd = []
        sAdCreativeFilepath = os.path.join(sDownloadPath, 'ad_creative.tsv')
        try:
            with open(sAdCreativeFilepath, 'r') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                for row in reader:
                    dictTempAd = {'id':row[0], 
                        'configured_status':row[1], 
                        'creative_id':row[2], 
                        'name':row[3], 
                        'link':row[4], 
                        'url_tags':row[5] }
                    lstAd.append(dictTempAd)
        except FileNotFoundError:
            oAccount = AdAccount(sAdAccountId) #'your-adaccount-id'
            try:
                ads = oAccount.get_ads(fields=[
                    Ad.Field.name,
                    #Ad.Field.tracking_specs,
                    Ad.Field.configured_status,
                    Ad.Field.creative,])
            except facebook_business.exceptions.FacebookRequestError as err:
                if err.http_status() == 400 and err.get_message() == 'Call was not successful' and err.api_error_code() == 190:
                    s_msg = err.api_error_message() + '\n' + \
                            'plz visit https://developers.facebook.com/apps/#app#id/marketing-api/tools/\n' + \
                            'token right select: ads_management, ads_read, read_insights -> get token\n' + \
                            'paste new token into /conf/fb_biz_config.ini'
                    if self._g_bDaemonEnv:  # for running on dbs.py only
                        logging.info(s_msg)
                        raise Exception('remove')
                    else:
                        self._printDebug(s_msg)
                        return
                else:
                    if self._g_bDaemonEnv:  # for running on dbs.py only
                        logging.info(err)
                        raise Exception('remove')
                    else:
                        self._printDebug(err)
                        return

            for oAds in ads:
                dictTempAd = {'id':oAds['id'], 'configured_status':oAds['configured_status'], 
                    'creative_id':oAds['creative']['id'], 'name':oAds['name']}
                #print('configured_status ' + oAds['configured_status'])
                #print('creative->creative_id ' + oAds['creative']['id'])
                #print('creative->id ' + oAds['creative']['id'])
                #print('id ' + oAds['id'])
                #print('name '+ oAds['name'])
                lstAd.append(dictTempAd)
                #for dictTrackingSpecs in oAds['tracking_specs']:
                #	for lstTrackingSpec in dictTrackingSpecs:
                #		print( lstTrackingSpec + ': ' + dictTrackingSpecs[lstTrackingSpec][0] )
                #	print( ' ' )
                #	# action.type: offsite_conversion, commerce_event, link_click, post_engagement
                #	# fb_pixel: 27074356419
                #	# page: 40134586627
                #	# post: 168ert1454410
                #	# post.wall: 4045686627
                #print( ' ' )
            #oAccount = AdAccount(sAdAccountId)
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
                for dictAd in lstAd:
                    if dictAd['creative_id'] == sCreativeId:
                        sHostInfo = 'error'
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
                                elif sStorySpec == 'page_id':
                                    if 'template_data' in dictCreatives['object_story_spec']:
                                        sHostInfo = dictCreatives['object_story_spec']['template_data']['link']
                                    elif 'link_data' in dictCreatives['object_story_spec']:
                                        sHostInfo = dictCreatives['object_story_spec']['link_data']['link']
                                    elif 'video_data' in dictCreatives['object_story_spec']:
                                        sHostInfo = dictCreatives['object_story_spec']['video_data']['call_to_action']['call_to_action']['value']['link']
                                elif sStorySpec == 'template_data':
                                    if 'template_data' in dictCreatives['object_story_spec']:
                                        sHostInfo = dictCreatives['object_story_spec']['template_data']['link']
                                    else:
                                        self._printDebug( dictCreatives['object_story_spec'] )
                                elif sStorySpec == 'instagram_actor_id':
                                    if 'template_data' in dictCreatives['object_story_spec']:
                                        sHostInfo = dictCreatives['object_story_spec']['template_data']['link']
                                    elif 'link_data' in dictCreatives['object_story_spec']:
                                        sHostInfo = dictCreatives['object_story_spec']['link_data']['link']
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
            # write data stream to file.
            with open(sAdCreativeFilepath, 'w', encoding='utf-8') as out:
                for dictAd in lstAd:
                    sRow = dictAd['id'] + '\t' + dictAd['configured_status'] + '\t' + dictAd['creative_id'] + '\t' + dictAd['name'].replace('\n', ' ') + '\t' + dictAd['link'] + '\t' + dictAd['url_tags'] + '\n'
                    out.write(sRow)
        fields = [
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
        sDataDate = dtDateDataRetrieval.strftime('%Y-%m-%d')
        sDataDateToLog =dtDateDataRetrieval.strftime('%Y%m%d')
        self._printDebug('--> '+ sFbBizAid +' will retrieve general report on ' + sDataDateToLog)
        
        try:
            sTsvFilename = sDataDateToLog + '_general.tsv'
            params = {
                'level': 'ad',
                #'filtering': [],
                'breakdowns': ['device_platform',],  #['gender','age'],
                'time_range': {'since':sDataDate,'until':sDataDate},  # 'date_preset': Insights.Preset.yesterday,
            }
            oInsights = AdAccount(sAdAccountId).get_insights(
                fields=fields,
               params=params,
            )
            try:
                if len(oInsights) > 0:
                    sTsvFilename = sDataDateToLog + '_general.tsv'
                    with open(os.path.join(sDownloadPath, sTsvFilename), 'w', encoding='utf-8') as out:
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
                                pass #self._printDebug('no conversion')
                            sAdIdFromInsight = dictInsight['ad_id']
                            for dictAd in lstAd:
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
            except:
                self._printDebug('exception occured')

            f = open(sEarliestFilepath, 'w')
            f.write(sDataDateToLog)
            f.close()
        except:
            s_msg = 'FACEBOOK api has reported weird error while processing sv account id: ' + sSvAcctId
            if self._g_bDaemonEnv:  # for running on dbs.py only
                logging.info(s_msg)
                raise Exception('remove')
            else:
                self._printDebug(s_msg)
                return


if __name__ == '__main__': # for console debugging and execution
    # dictPluginParams = {'config_loc':'2/1', 'earliest_date':'20180223', 'latest_date':'20180424'}
    nCliParams = len(sys.argv)
    if nCliParams > 2:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('fb_get_period')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
            pass
    else:
        print('warning! [config_loc] [earliest_date] [latest_date] params are required for console execution.')
