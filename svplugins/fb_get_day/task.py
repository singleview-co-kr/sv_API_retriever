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

# sys.path.append('/usr/lib/python3.7/site-packages/facebook_business') # Replace this with the place you installed facebookads using pip
#sys.path.append('/opt/homebrew/lib/python2.7/site-packages/facebook_business-3.0.0-py2.7.egg-info') # same as above
import facebook_business
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    import sv_object, sv_plugin
    sys.path.append('../../conf') # singleview config
    import fb_biz_config
else: # for platform running
    from svcommon import sv_object, sv_plugin
    # singleview config
    from conf import fb_biz_config


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        self._g_sVersion = '1.0.2'
        self._g_sLastModifiedDate = '19th, Oct 2021'
        self._g_oLogger = logging.getLogger(__name__ + ' v'+self._g_sVersion)

    def do_task(self, o_callback):
        oResp = self._task_pre_proc(o_callback)
        dict_acct_info = oResp['variables']['acct_info']
        if dict_acct_info is None:
            self._printDebug('stop -> invalid config_loc')
            return
        
        s_sv_acct_id = list(dict_acct_info.keys())[0]
        s_acct_title = dict_acct_info[s_sv_acct_id]['account_title']
        s_fb_biz_aid = dict_acct_info[s_sv_acct_id]['fb_biz_aid']
        if s_fb_biz_aid == '':
            self._printDebug('stop -> no business account id')
            return

        self._printDebug('fb_get_day plugin launched')
        self._printDebug(s_fb_biz_aid)
        try:
            self.__getFbBusinessRaw(s_sv_acct_id, s_acct_title, s_fb_biz_aid )
            pass
        except TypeError as error:
            # Handle errors in constructing a query.
            self._printDebug(('There was an error in constructing your query : %s' % error))
        self._printDebug('fb_get_day plugin finished')

        self._task_post_proc(o_callback)

    def __getFbBusinessRaw(self, sSvAcctId, sAcctTitle, sFbBizAid):
        sDownloadPath = os.path.join(self._g_sAbsRootPath, 'files', sSvAcctId, sAcctTitle, 'fb_biz', sFbBizAid, 'data')
        if os.path.isdir(sDownloadPath) == False:
            os.makedirs(sDownloadPath)
        
        s_conf_path_abs = os.path.join(self._g_sAbsRootPath, 'files', sSvAcctId, sAcctTitle, 'fb_biz', sFbBizAid, 'conf')
        if os.path.isdir(s_conf_path_abs) == False:
            os.makedirs(s_conf_path_abs)

        try:
            sLatestFilepath = os.path.join(s_conf_path_abs, 'general.latest')
            f = open(sLatestFilepath, 'r')
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
        
        sAccessToken = fb_biz_config.ACCESS_TOKEN
        sAdAccountId = 'act_'+sFbBizAid
        FacebookAdsApi.init(access_token=sAccessToken, api_version='v10.0')
        lstAd = []
        oAccount = AdAccount(sAdAccountId) #'your-adaccount-id'
        self._printDebug('error occured')
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
                                'paste new token into /conf/fb_biz_config.py')
            return
        
        for oAds in ads:
            dictTempAd = {'id':oAds['id'], 'configured_status':oAds['configured_status'], 'creative_id':oAds['creative']['id'], 'name':oAds['name'] }
            #print('configured_status ' + oAds['configured_status'])
            #print('creative->creative_id ' + oAds['creative']['creative_id'])
            #print('creative->id ' + oAds['creative']['id'])
            #print('id ' + oAds['id'])
            #print('name '+ oAds['name'])
            lstAd.append( dictTempAd )

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
        #with open(sDownloadPath+'/'+sTsvFilename, 'w', encoding='utf-8' ) as out:
        #	for dictAd in lstAd:
        #		sRow = dictAd['id'] + '\t' + dictAd['configured_status'] + '\t' + dictAd['creative_id'] + '\t' + dictAd['name'] + '\t' + dictAd['link'] + '\t' + dictAd['url_tags'] + '\n'
        #		out.write( sRow )
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

        while self._continue_iteration(): # loop for each report date
            try:
                dtRetrieval = list(dictDateQueue.keys())[list(dictDateQueue.values()).index(0)] # find unhandled report task
            except ValueError:
                break
                
            sDataDate = dtRetrieval.strftime('%Y-%m-%d')
            sDataDateToLog = dtRetrieval.strftime('%Y%m%d')

            self._printDebug( '--> '+ sFbBizAid +' will retrieve general report on ' + sDataDate)

            params = {
                'level': 'ad',
                #'filtering': [],
                'breakdowns': ['device_platform',],  #['gender','age'],
                'time_range': {'since':sDataDate,'until':sDataDate}, # 'date_preset': Insights.Preset.yesterday,
            }
            oInsights = AdAccount(sAdAccountId).get_insights(
                fields=fields,
                params=params,
            )
            try:
                if len(oInsights) > 0:
                    sTsvFilename = sDataDateToLog + '_general.tsv'
                    with open(sDownloadPath+'/'+sTsvFilename, 'w', encoding='utf-8') as out:
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
                else:
                    self._printDebug( 'WARNING! no data detected!\nstop querying if you do not spend.\nfb api does not like free querying\nthey might block API access temporarily.')
                
                try:
                    f = open(sLatestFilepath, 'w')
                    f.write(sDataDateToLog)
                    f.close()
                except PermissionError:
                    break
                    
                dictDateQueue[dtRetrieval] = 1
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
        print('warning! [analytical_namespace] [config_loc] params are required for console execution.')
 