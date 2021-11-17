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
import os
import sys
import logging
import configparser # https://docs.python.org/3/library/configparser.html
import tweepy  # pip install tweepy
from datetime import datetime
# import pytz

# 3rd party library

# singleview config
if __name__ == 'svcommon.sv_twitter': # for platform running
    from conf import basic_config
    from svcommon import sv_object
elif __name__ == 'sv_twitter': # for plugin console debugging
    sys.path.append('../../conf')
    import basic_config
    import sv_object
elif __name__ == '__main__': # for class console debugging
    sys.path.append('../conf')
    import basic_config
    import sv_object

    
class svTwitter(sv_object.ISvObject):
    """  """
    __g_oTwitterApi = None
    # __g_oConfig = None
    __g_bAvailable = False

    def __init__(self):
        # self.__g_oConfig = configparser.ConfigParser()
        o_config = configparser.ConfigParser()
        self._g_oLogger = logging.getLogger(__file__)
        s_twitter_config_file = os.path.join(basic_config.ABSOLUTE_PATH_BOT, 'conf', 'twitter_config.ini')
        
        try:
            with open(s_twitter_config_file) as f:
                o_config.read_file(f)
                self.__g_bAvailable = True
        except IOError:
            self._printDebug('twitter_config.ini does not exist')

        if self.__g_bAvailable:
            o_config.read(s_twitter_config_file)

        try:  # attempt authentication
            # create OAuthHandler object
            auth = tweepy.OAuthHandler(o_config['COMMON']['consumer_key'], o_config['COMMON']['consumer_secret'])
            # set access token and secret
            auth.set_access_token(o_config['COMMON']['access_token'], o_config['COMMON']['access_token_secret'])
            # create tweepy API object to fetch tweets
            self.__g_oTwitterApi = tweepy.API(auth, wait_on_rate_limit=True)
            # dict_rate_limit_context = self.__g_oTwitterApi.rate_limit_status()
            self._printDebug('Authentication Succeeded')
        except:
            self._printDebug('Error: Twitter API Authentication Failed')
        finally:
            del o_config

    def searchQuery(self, s_query, n_since_id=None, n_max_id=None, s_until=None, n_twt_limit=1):
        """
        since_id - Returns results with an ID greater than (that is, more recent than) the specified ID. 
        There are limits to the number of Tweets which can be accessed through the API. 
        If the limit of Tweets has occured since the since_id, the since_id will be forced to the oldest ID available. 
        max_id - Returns results with an ID less than (that is, older than) or equal to the specified ID.
        until - Returns tweets created before the given date.... Date should be formatted as YYYY-MM-DD
        """
        lst_status = []
        if not self.__g_bAvailable:
            self._printDebug('execution denied')
            return lst_status

        if s_until:
            try:
                datetime.strptime(s_until, '%Y-%m-%d')
            except ValueError:
                self._printDebug("Incorrect data format, should be YYYY-MM-DD")
                return lst_status
        else:
            s_until = datetime.now().strftime("%Y-%m-%d")  # limit to yesterday tweets

        n_cnt = 0
        if len(s_query):  # keyword-only tweets that matches keyword will be fetched
            # utc = pytz.UTC
            # dt_start = datetime.datetime(2021, 11, 3, 0, 0, 0).replace(tzinfo=utc)
            # del utc
            try:
                for o_tweet in tweepy.Cursor(self.__g_oTwitterApi.search_tweets, 
                                            q=s_query, lang="ko", tweet_mode="extended", result_type="recent", 
                                            since_id=n_since_id,  # 육아 1458220286151921666, 
                                            max_id=n_max_id,  # 육아 1458220438266388494, 
                                            until=s_until  # '2021-11-10'
                                            ).items(n_twt_limit):
                    n_cnt = n_cnt + 1
                    # if dt_start < o_tweet.created_at <= dt_end:
                    s_tweet_text = str(o_tweet.full_text[:30].lower())
                    dict_rtwit = None
                    dict_qtwit = None
                    if s_tweet_text.startswith("rt @"):  # check if the tweet is a retweet
                        dict_rtwit = {'s_status_id': o_tweet.retweeted_status.id_str, 
                                        's_user_id': o_tweet.retweeted_status.user.id_str,
                                        'n_user_followers_cnt': o_tweet.retweeted_status.user.followers_count,
                                        'n_user_friends_cnt': o_tweet.retweeted_status.user.friends_count,
                                        'n_user_favourites_cnt': o_tweet.retweeted_status.user.favourites_count,
                                        'n_retweet_cnt': o_tweet.retweeted_status.retweet_count,
                                        'n_favorite_cnt': o_tweet.retweeted_status.favorite_count,
                                        's_full_text': o_tweet.retweeted_status.full_text, 'dt_created_at': o_tweet.retweeted_status.created_at}
                        if o_tweet.retweeted_status.is_quote_status:
                            dict_qtwit = {'s_status_id': o_tweet.retweeted_status.quoted_status.id_str, 
                                            's_user_id': o_tweet.retweeted_status.quoted_status.user.id_str,
                                            'n_user_followers_cnt': o_tweet.retweeted_status.quoted_status.user.followers_count,
                                            'n_user_friends_cnt': o_tweet.retweeted_status.quoted_status.user.friends_count,
                                            'n_user_favourites_cnt': o_tweet.retweeted_status.quoted_status.user.favourites_count,
                                            'n_retweet_cnt': o_tweet.retweeted_status.quoted_status.retweet_count,
                                            'n_favorite_cnt': o_tweet.retweeted_status.quoted_status.favorite_count,
                                            's_full_text': o_tweet.retweeted_status.quoted_status.full_text, 'dt_created_at': o_tweet.retweeted_status.quoted_status.created_at}
                    # o_tweet.in_reply_to_status_id_str # Replying to 에서는 replying status full text 미포함
                    # o_tweet.in_reply_to_user_id_str
                    lst_status.append({'s_status_id': o_tweet.id_str, 
                                        's_user_id': o_tweet.user.id_str,
                                        'n_user_followers_cnt': o_tweet.user.followers_count,
                                        'n_user_friends_cnt': o_tweet.user.friends_count,
                                        'n_user_favourites_cnt': o_tweet.user.favourites_count,
                                        'n_retweet_cnt': o_tweet.retweet_count,
                                        'n_favorite_cnt': o_tweet.favorite_count,
                                        's_full_text': o_tweet.full_text, 'dt_created_at': o_tweet.created_at,
                                        'dict_rtwit': dict_rtwit, 'dict_qtwit': dict_qtwit})
            except Exception as e:  # e.response e.api_errors e.api_codes e.api_messages
                if 215 in e.api_codes:  # Bad Authentication data.
                    self._printDebug('invalid API key')
        else:
            self._printDebug(__file__ + ' has requested to send blank message!')

        return lst_status


# if __name__ == '__main__': # for console debugging
#     oSvTwitter = svTwitter()
#     # n_twt_limit = 110
#     # since_id=1458220286151921666,  # 육아
#     # max_id=1458220438266388494,  # 육아
#     lst_status = oSvTwitter.searchQuery(s_query='육아', n_twt_limit = 501) #, n_since_id=1414180478970580991, n_max_id=145978385340120675)
#     # for dict_single_status in lst_status:
#     #     print('https://twitter.com/i/web/status/' + dict_single_status['s_status_id'])

#     #     if dict_single_status['dict_rtwit']:
#     #         print(dict_single_status['s_user_id'] + ' has retwitted status by user ' + dict_single_status['dict_rtwit']['s_user_id'])
#     #         print('https://twitter.com/i/web/status/' + dict_single_status['dict_rtwit']['s_status_id'])
#     #         print(dict_single_status['dict_rtwit']['s_full_text'] + ' at ' + str(dict_single_status['dict_rtwit']['dt_created_at']))
#     #         if dict_single_status['dict_qtwit']:
#     #             print('this twit quoted status by user ' + dict_single_status['dict_qtwit']['s_user_id'])
#     #             print('https://twitter.com/i/web/status/' + dict_single_status['dict_qtwit']['s_status_id'])
#     #             print(dict_single_status['dict_qtwit']['s_full_text'] + ' at ' + str(dict_single_status['dict_qtwit']['dt_created_at']))
#     #     else:
#     #         print(dict_single_status['s_user_id'] + ' retwitted')
#     #         print(dict_single_status['s_full_text'] + ' at ' + str(dict_single_status['dt_created_at']))

#     #     print('')
    
#     print(str(len(lst_status)) + ' tweets')
