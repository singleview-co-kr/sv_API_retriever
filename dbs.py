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
import sys
import time
import logging  #https://docs.python.org/3.3/howto/logging.html
import logging.config  #https://docs.python.org/3.3/howto/logging.html

# 3rd party library
import daemonocle  # https://programtalk.com/vs2/?source=python/6398/daemonocle/tests/test_actions.py
# https://pypi.python.org/pypi/daemonocle
import click # command line interface related
from daemonocle.cli import DaemonCLI # command line interface related; connected with daemonocle
from apscheduler.schedulers.background import BackgroundScheduler # APscheduler
import apscheduler.events as events
from decouple import config

# singleview library
from svcommon import sv_mysql
from svcommon import sv_plugin
from svcommon import sv_events
from svcommon import sv_slack

from svinitialize import sv_console_init
o_sv_console_init = sv_console_init.svInitialize(sys.argv)
s_msg = o_sv_console_init.execute()
del o_sv_console_init  
if s_msg != 'pass':
    sys.exit(0)

g_oScheduler = None
g_sVersion = '1.3.3'
g_sLastModifiedDate = '5th, Jun 2023'
g_sAbsPathBot = config('ABSOLUTE_PATH_BOT')


class SetSvDaemon(daemonocle.Daemon):
    """ this class seems to be created by different process with main(), that is configuration run by main() is not working in this code block 
        main() is not executed even if you run the CLI [python3.6 crawler.py banana] """
    # basic usage: python dbs.py start/stop
    @daemonocle.expose_action
    def send(self):
        """initialize crawler running environment"""
        # self.__get_logger()
        # _arouse_dbo('1')

    # @daemonocle.expose_action
    # def gaauth(self):
    #     # move this method to ga job plugin?
    #     print( '1. place ./client_secret.json downloaded from google to root directory(where ./dbs.py is located)' )
    #     print( '2. run python dbs.py gaauth' )
    #     print( '3. move newly created analytics.dat to ./conf/google_analytics.dat directory manually' )
    #     print( '4. move client_secret.json to ./conf/google_client_secret.json' )
    #     """ to get auth for the first time, get console auth with arg  --noauth_local_webserver """
    #     sys.argv[1] = '--noauth_local_webserver'
    #     from googleapiclient import sample_tools
    #     service, flags = sample_tools.init(
    #         sys.argv, 'analytics', 'v3', __doc__, __file__,
    #         scope='https://www.googleapis.com/auth/analytics.readonly')

    # @daemonocle.expose_action
    # def adwrefreshtoken(self):
    #     # move this method to aw job plugin?
    #     print('1. run ./svplugins/aw_get_day/generate_refresh_token.py')

    @daemonocle.expose_action
    def init(self):
        """initialize crawler running environment"""
        # should separate dbo.py and dbs.py initialization
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.initialize()

    @daemonocle.expose_action
    def version(self):
        """initialize crawler running environment"""
        print('doing-by-schedule bot all rights reserved by singleview.co.kr')
        print('Version: '+ g_sVersion + ' modified on ' + g_sLastModifiedDate)


def cb_shutdown(message, code):
    logging.info(__file__ + ' v' + g_sVersion + ' has been shutdown')
    logging.debug(message)
    o_slack = sv_slack.SvSlack('dbs')
    o_slack.sendMsg('bot has been shutdown')
    del o_slack


@click.command(cls=DaemonCLI, daemon_class=SetSvDaemon, daemon_params={'shutdown_callback': cb_shutdown, 'pidfile': './misc/dbs.pid'})
def main():
    """ This is singleview doing-by-schedule bot. It assists to run various regular job."""
    #global g_oScheduler
    #g_oScheduler.pause() #g_oScheduler.resume()
    # create logger
    logging.basicConfig(
        filename= g_sAbsPathBot + '/log/dbs.log',
        level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s',
    )
    logging.info(__file__ + ' v' + g_sVersion + ' has been started')
    
    _start_scheduler()
    _sync_job_from_mysql()

    while True:  # 'daemon' code
        time.sleep(6) # process CPU shr % will go up to 100% w/o this execution


def _start_scheduler():
    global g_oScheduler
    # The "apscheduler." prefix is hard coded
    # need to install sqlalchemy module first refer to http://docs.sqlalchemy.org/en/latest/intro.html
    b_devel_mode = False
    try:
        if config('DEBUG'):
            b_devel_mode = True
    except AttributeError as e:
        pass

    if b_devel_mode:
        g_oScheduler = BackgroundScheduler()
    else:
        g_oScheduler = BackgroundScheduler({
            'apscheduler.jobstores.default': {  
                'type': 'sqlalchemy',
                'url': 'sqlite:///' + config('ABSOLUTE_PATH_BOT') + '/misc/jobs.sqlite' # 'sqlite:///' is designated prefix
            },
            'apscheduler.executors.default': {
                'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
                'max_workers': '20'
            },
            'apscheduler.job_defaults.coalesce': 'false',
            'apscheduler.job_defaults.max_instances': '3',
            'apscheduler.timezone': 'Asia/Seoul', 
        })
    g_oScheduler.add_listener(_my_listener, events.EVENT_JOB_EXECUTED | events.EVENT_JOB_ERROR)
    g_oScheduler.start()
    o_slack = sv_slack.SvSlack('dbs')
    o_slack.sendMsg('bot has been started')
    del o_slack


def _my_listener(event):
    '''Listens to completed job'''
    # http://apscheduler.readthedocs.io/en/latest/modules/events.html
    # http://nullege.com/codes/show/src%40c%40k%40ckan-service-provider-HEAD%40ckanserviceprovider%40web.py/69/apscheduler.events.EVENT_JOB_ERROR/python
    o_logger = logging.getLogger(__file__)
    o_slack = sv_slack.SvSlack('dbs')
    #o_logger.debug(event.code)  # raised event code defined by APS; normal completion designates 4096 = events.EVENT_JOB_EXECUTED
    #o_logger.debug(event.exception) # raised event code defined by sys.exit(); normal completion designates None
    if str(event.exception) == sv_events.EVENT_JOB_SHOULD_BE_REMOVED:
        o_logger.debug('######_my_listener will remove job, id: ' + event.job_id + ' ##########')
        o_slack.sendMsg('_my_listener has removed job, id: ' + event.job_id)
        g_oScheduler.remove_job(event.job_id)
    elif str(event.exception) == sv_events.EVENT_JOB_COMPLETED:
        o_logger.debug('######_my_listener will remove job, id: ' + event.job_id + ' ##########')
        o_slack.sendMsg('_my_listener has removed job, id: ' + event.job_id)
        g_oScheduler.remove_job(event.job_id)
        o_logger.debug('######_my_listener will togle job, id: ' + event.job_id + ' from table ##########')
        o_slack.sendMsg('_my_listener has toggled job, id: ' + event.job_id + ' from table' )
        with sv_mysql.SvMySql() as o_sv_mysql: # to enforce follow strict mysql connection mgmt
            o_sv_mysql.initialize()
            o_sv_mysql.execute_query('updateJobIsActive', 0, event.job_id)
            o_sv_mysql.commit()

    del o_slack


def _sync_job_from_mysql():
    """
    interval trigger params
        weeks (int) – number of weeks to wait
        days (int) – number of days to wait
        hours (int) – number of hours to wait
        minutes (int) – number of minutes to wait
        seconds (int) – number of seconds to wait
        start_date (datetime|str) – starting point for the interval calculation
        end_date (datetime|str) – latest possible date/time to trigger on ex) end_date='2014-05-30'
        timezone (datetime.tzinfo|str) – time zone to use for the date/time calculations
        jitter(int|None) – advance or delay the job execution by jitter seconds at most. -> but error occured if applied
    cron trigger params
        year (int|str) – 4-digit year
        month (int|str) – month (1-12)
        day (int|str) – day of the (1-31)
        week (int|str) – ISO week (1-53)
        day_of_week (int|str) – number or name of weekday (0-6 or mon,tue,wed,thu,fri,sat,sun)
        hour (int|str) – hour (0-23)
        minute (int|str) – minute (0-59)
        second (int|str) – second (0-59)
        start_date (datetime|str) – earliest possible date/time to trigger on (inclusive)
        end_date (datetime|str) – latest possible date/time to trigger on (inclusive) ex) end_date='2018-05-30'
        timezone (datetime.tzinfo|str) – time zone to use for the date/time calculations (defaults to scheduler timezone)
        jitter (int|None) – advance or delay the job execution by jitter seconds at most.
            Run the `job_function` every hour with an extra-delay picked randomly in a [-120,+120] seconds window.
            sched.add_job(job_function, 'interval', hours=1, jitter=120)
        Expression -- Field -- Description
        * -- any -- Fire on every value
        */a -- any -- Fire every a values, starting from the minimum
        a-b -- any -- Fire on any value within the a-b range (a must be smaller than b)
        a-b/c -- any -- Fire every c values within the a-b range
        xth y -- day -- Fire on the x -th occurrence of weekday y within the month
        last x -- day -- Fire on the last occurrence of weekday x within the month
        last -- day -- Fire on the last day within the month
        x,y,z -- any -- Fire on any matching expression; can combine any number of any of the above expressions
    """
    o_logger = logging.getLogger(__file__)
    with sv_mysql.SvMySql() as o_sv_mysql: # to enforce follow strict mysql connection mgmt
        o_sv_mysql.initialize()
        lst_raw_mysql_jobs = o_sv_mysql.execute_query('getJobList', 1)  # 1 means active

    # remove any job existed in Mysql but application_date is earlier than modification_date; remove updated job to add newly
    for dict_mysql_job in lst_raw_mysql_jobs:
        n_modified_date = 0
        n_applied_date = 0
        if dict_mysql_job['dt_mod']:
            n_modified_date = int(dict_mysql_job['dt_mod'].strftime('%Y%m%d'))
        if dict_mysql_job['dt_applied']:
            n_applied_date = int(dict_mysql_job['dt_applied'].strftime('%Y%m%d'))
        
        if n_modified_date > n_applied_date:
            o_logger.debug(str(dict_mysql_job['id']) + ' should be updated')
            try:
                g_oScheduler.remove_job(str(dict_mysql_job['id']))
            except Exception as inst:
                o_logger.debug(inst) # print debug msg and ignore if job is not existed

    # arrange job list retrieved from Mysql
    dict_jobs_in_mysql = {}
    for dict_mysql_job in lst_raw_mysql_jobs:
        dict_mysql_job[str(dict_mysql_job['id'])] = dict_mysql_job['s_job_title']
        dict_jobs_in_mysql[str(dict_mysql_job['id'])] = dict_mysql_job['s_job_title']
    
    dict_running_jobs = g_oScheduler.get_jobs() 
    dict_jobs_in_sqlite = {}
    for job in dict_running_jobs:
        dict_jobs_in_sqlite[job.id] = job.name
    del dict_running_jobs
    
    # remove any job existed in Sqllite but not in Mysql
    for s_job_id_in_sqlite in dict_jobs_in_sqlite.keys():
        try:
            dict_jobs_in_mysql[s_job_id_in_sqlite]
        except KeyError:
            o_logger.debug(s_job_id_in_sqlite + ' is not existed in dict_mysql_job - will be deleted')
            g_oScheduler.remove_job(s_job_id_in_sqlite)
        else:
            o_logger.debug(s_job_id_in_sqlite + ' exists in dict_mysql_job - pass')

    # add any job existed in Mysql but not in Sqllite
    dict_trigger_type = {'i': 'interval', 'c': 'cron'}  # should be streamlined with svdaemon.models
    o_svplugin_validation = sv_plugin.SvPluginValidation()
    for s_mysql_job in dict_jobs_in_mysql:
        if s_mysql_job in dict_jobs_in_sqlite:
            o_logger.debug(s_mysql_job + ' exists in jobIdInSqlite - pass')
            continue
        o_logger.debug(s_mysql_job + ' is not existed in jobIdInSqlite - will be added')
        for dict_row in lst_raw_mysql_jobs:
            if s_mysql_job == str(dict_row['id']):
                if o_svplugin_validation.validate(dict_row['s_plugin_name']): #add job if valid
                    dict_trigger_params = _parse_trigger_params(dict_row['s_trigger_type'], dict_row['s_trigger_params'])
                    # o_logger.debug(dict_trigger_params)
                    # http://apscheduler.readthedocs.io/en/latest/modules/schedulers/base.html
                    u_id = str(dict_row['id'])
                    u_name = dict_row['s_job_title']
                    u_start_date = dict_row['date_start'] or '1970-01-01'
                    u_end_date = dict_row['date_end'] or '2200-12-31'
                    s_config_loc_param = 'config_loc=' + str(dict_row['sv_acct_id']) + '/' + str(dict_row['sv_brand_id'])
                    if dict_row['s_trigger_type'] == 'i':  # interval trigger
                        u_wks = int(dict_trigger_params['weeks'])
                        u_days = int(dict_trigger_params['days'])
                        u_hrs = int(dict_trigger_params['hours'])
                        u_mins = int(dict_trigger_params['minutes'])
                        u_secs = int(dict_trigger_params['seconds'])
                        # http://apscheduler.readthedocs.io/en/latest/modules/triggers/interval.html
                        g_oScheduler.add_job(sv_plugin.SvPluginDaemonJob, 'interval',
                            [dict_row['s_plugin_name'], s_config_loc_param, dict_row['s_plugin_params']],
                            weeks = u_wks, days = u_days, hours = u_hrs, minutes = u_mins, seconds = u_secs, 
                            id = u_id, name = u_name, start_date = u_start_date, end_date = u_end_date)
                    elif dict_row['s_trigger_type'] == 'c':  # cron trigger
                        u_yr = dict_trigger_params['year']
                        u_mo = dict_trigger_params['month']
                        u_wk = dict_trigger_params['week']
                        u_dow = dict_trigger_params['day_of_week']
                        u_day = dict_trigger_params['day']
                        u_hr = dict_trigger_params['hour']
                        u_min = dict_trigger_params['minute']
                        u_sec = dict_trigger_params['second']
                        # http://apscheduler.readthedocs.io/en/latest/modules/triggers/cron.html
                        g_oScheduler.add_job(sv_plugin.SvPluginDaemonJob, 'cron',
                            [dict_row['s_plugin_name'], s_config_loc_param, dict_row['s_plugin_params']],
                            year = u_yr, month = u_mo, week = u_wk, day_of_week = u_dow, day = u_day, hour = u_hr, minute = u_min, second = u_sec,
                            id = u_id, name = u_name, start_date = u_start_date, end_date = u_end_date )
                    with sv_mysql.SvMySql() as o_sv_mysql: # to enforce follow strict mysql connection mgmt
                        o_sv_mysql.initialize()
                        o_sv_mysql.execute_query('updateJobAppliedDate', dict_row['id'])
                        o_sv_mysql.commit()  # update stmt on dbs.py requires explicit commit(); dont know why
                    
                    o_logger.debug(u_name + ' with ' + dict_trigger_type[dict_row['s_trigger_type']] + ' trigger has been added')
                    del dict_trigger_params
                else:
                    o_logger.debug("plugin '%s' not valid" % dict_row['s_plugin_name'])                
    del o_svplugin_validation
    del lst_raw_mysql_jobs
    del dict_jobs_in_sqlite
    del dict_jobs_in_mysql


def _parse_trigger_params(s_trig_type, s_trigger_params):
    """ should be streamlined with svdaemon.models """
    if s_trig_type == 'i':  # TriggerType.INTERVAL:
        dict_trig_param = {'weeks':0, 'days':0, 'hours':0, 'minutes':0, 'seconds':0}
        for s_line in s_trigger_params.splitlines():
            lst_param = s_line.split('=')
            if lst_param[0] in dict_trig_param.keys():
                dict_trig_param[lst_param[0]] = lst_param[1]
    elif s_trig_type == 'c':  # TriggerType.CRON:
        dict_trig_param = {'year':0, 'month':0, 'week':0, 'day':0, 'day_of_week':0, 'hour':0, 'minute':0, 'second':0}
        for s_line in s_trigger_params.splitlines():
            lst_param = s_line.split('=')
            if lst_param[0] in dict_trig_param.keys():
                dict_trig_param[lst_param[0]] = lst_param[1]
    return dict_trig_param


if __name__ == '__main__':
    main()
