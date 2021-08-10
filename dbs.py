# -*- coding: UTF-8 -*-
# doing by schedule bot
# singleview config
from conf import basic_config

# standard library
import sys
import time
from datetime import datetime
import http.client # https://docs.python.org/3/library/http.client.html
import base64
import logging, logging.config  #https://docs.python.org/3.3/howto/logging.html
import glob
import os, signal

# 3rd party library
import daemonocle  # https://programtalk.com/vs2/?source=python/6398/daemonocle/tests/test_actions.py
# https://pypi.python.org/pypi/daemonocle
import click # command line interface related
from daemonocle.cli import DaemonCLI # command line interface related; connected with daemonocle
from apscheduler.schedulers.background import BackgroundScheduler # APscheduler
import apscheduler.events as events
import simplejson as json

# singleview library
from classes import sv_mysql
from classes import sv_plugin
from classes import sv_events
from classes import sv_slack

g_oScheduler = None
g_sVersion = '1.3.0'
g_sLastModifiedDate = '17th, Jul 2021'

class setSvDaemon(daemonocle.Daemon):
    # basic usage: python3.6 dbs.py start/stop
	@daemonocle.expose_action
	def send(self):
		"""initialize crawler running environment"""
		self.__getLogger()
		_arouseDbo('1')
	
	@daemonocle.expose_action
	def gaauth(self):
		# move this method to ga job plugin?
		print( '1. place ./client_secret.json downloaded from google to root directory(where ./dbs.py is located)' )
		print( '2. run python dbs.py gaauth' )
		print( '3. move newly created analytics.dat to ./conf/google_analytics.dat directory manually' )
		print( '4. move client_secret.json to ./conf/google_client_secret.json' )
		""" to get auth for the first time, get console auth with arg  --noauth_local_webserver """
		sys.argv[1] = '--noauth_local_webserver'
		from googleapiclient import sample_tools
		service, flags = sample_tools.init(
			sys.argv, 'analytics', 'v3', __doc__, __file__,
			scope='https://www.googleapis.com/auth/analytics.readonly')
	
	@daemonocle.expose_action
	def adwrefreshtoken(self):
		# move this method to aw job plugin?
		print( '1. run ./job_plugins/aw_get_day/generate_refresh_token.py')

	@daemonocle.expose_action
	def init(self):
		"""initialize crawler running environment"""
		# should separate dbo.py and dbs.py initialization
		#oSvMysql = sv_mysql.SvMySql()
		#oSvMysql.initialize()
		with sv_mysql.SvMySql() as oSvMysql:
			oSvMysql.initialize()

	@daemonocle.expose_action
	def version(self):
		"""initialize crawler running environment"""
		print( 'doing-by-schedule bot all rights reserved by singleview.co.kr' )
		print( 'Version: '+ g_sVersion + ' modified on ' + g_sLastModifiedDate )
	
	def __getLogger(self):
		""" this class seems to be created by different process with main(), that is configuration run by main() is not working in this code block 
			main() is not executed even if you run the CLI [python3.6 crawler.py banana] """
		logging.config.fileConfig( basic_config.ABSOLUTE_PATH_BOT+'/conf/logging_dbs_exposure_action.conf') # https://docs.python.org/3.3/library/logging.config.html
		return logging.getLogger('exposure_action')

def cb_shutdown(message, code):
	logging.info(__file__ + ' v' + g_sVersion + ' has been shutdown')
	logging.debug(message)
	oSvSlack = sv_slack.svSlack('dbs')
	oSvSlack.sendMsg('bot has been shutdown')

@click.command(cls=DaemonCLI, daemon_class=setSvDaemon, daemon_params={'shutdown_callback': cb_shutdown, 'pidfile': './misc/dbs.pid'})
def main():
	""" This is singleview doing-by-schedule bot. It assists to run various regular job."""
	#global g_oScheduler
	#g_oScheduler.pause() #g_oScheduler.resume()

	# create logger
	logging.config.fileConfig( basic_config.ABSOLUTE_PATH_BOT+'/conf/logging_dbs.conf') # https://docs.python.org/3.3/library/logging.config.html
	oLogger = logging.getLogger(__file__)
	oLogger.info(__file__ + ' v' + g_sVersion + ' has been started')
	
	_startScheduler()
	_syncJobFromMysqlDb()
	
	while True:  # 'daemon' code
		time.sleep(6) # process CPU shr % will go up to 100% w/o this execution

def _startScheduler():
	global g_oScheduler
	# The "apscheduler." prefix is hard coded
	# need to install sqlalchemy module first refer to http://docs.sqlalchemy.org/en/latest/intro.html
	isDevelopmentMode = False
	try:
		if( basic_config.RUN_MODE == 'devel' ):
			isDevelopmentMode = True
	except AttributeError as e:
		pass

	if( isDevelopmentMode ):
		g_oScheduler = BackgroundScheduler()
	else:
		g_oScheduler = BackgroundScheduler({
			'apscheduler.jobstores.default': {  
				'type': 'sqlalchemy',
				'url': 'sqlite:///' + basic_config.ABSOLUTE_PATH_BOT+ '/misc/jobs.sqlite' # 'sqlite:///' is designated prefix
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
	oSvSlack = sv_slack.svSlack('dbs')
	oSvSlack.sendMsg('bot has been started')

def _arouseDbo(nParentJobId):
	oLogger = logging.getLogger(__file__)
	try:
		with open (basic_config.ABSOLUTE_PATH_BOT+ '/misc/dbo.pid', 'r') as myfile:
			sPid = myfile.read()
			nPid = int(sPid)
	except FileNotFoundError:
		nPid = 0
	
	# if dbo.py PID exists
	#oLogger.debug(nPid)
	if( nPid > 0 ):
		oSvSlack = sv_slack.svSlack('dbs')
		with sv_mysql.SvMySql() as oSvMysql: # to enforce follow strict mysql connection mgmt
			lstRootJob = oSvMysql.executeQuery('getJobBySrl', nParentJobId )
			#oLogger.debug(lstRootJob)
			sCallerPluginName = lstRootJob[0]['plugin_name']
			sCallerPluginParams = lstRootJob[0]['plugin_params']
		
			# check if caller plugin is valid
			oSvPluginValidation = sv_plugin.svPluginValidation()
			dictCallerValidation = oSvPluginValidation.validatePlugin( sCallerPluginName )
			if dictCallerValidation['validation']:
				lstFollowupJobPluginName = dictCallerValidation['followup_job']
				#oLogger.debug(lstFollowupJobPluginName)				
				
				if lstFollowupJobPluginName[0] is not 'no':
					bValidFollwupJobPlugin = True
					for sFollowupJobPluginName in lstFollowupJobPluginName:
						oLogger.debug(sFollowupJobPluginName)
						dictFollowupJobValidation = oSvPluginValidation.validatePlugin( sFollowupJobPluginName )
						# check if followup plugin is valid
						if dictFollowupJobValidation['validation'] is False:
							#oLogger.debug(dictFollowupJobValidation)
							bValidFollwupJobPlugin = False
					
					if bValidFollwupJobPlugin:
						try:
							sCurDatetimeStamp = time.strftime('%Y%m%d%H%M%S')
							oSvMysql.executeQuery('insertHandoverOnDbs', nParentJobId, sCallerPluginName, sCallerPluginParams, sCurDatetimeStamp )
							time.sleep(0.5)
							os.kill(nPid, signal.SIGUSR1)
							oLogger.debug('SIGUSR1 has been sent to ' + sPid)
						except ProcessLookupError:
							oLogger.debug('dbo.py has been called but is not running...')
							oSvSlack.sendMsg('dbo.py has been called but is not running...')
					else:
						g_oScheduler.remove_job(nParentJobId)
						oLogger.debug('######_arouseDbo has removed job, id: ' + nParentJobId + ' as its followup.conf contains wrong definition ##########')
						oSvSlack.sendMsg('_arouseDbo has removed job, id: ' + nParentJobId + ' as its followup.conf contains wrong definition')
				else:
					oLogger.debug('no follow up')
					oSvSlack.sendMsg('no follow up')

def _my_listener(event):
	'''Listens to completed job'''
	# http://apscheduler.readthedocs.io/en/latest/modules/events.html
	# http://nullege.com/codes/show/src%40c%40k%40ckan-service-provider-HEAD%40ckanserviceprovider%40web.py/69/apscheduler.events.EVENT_JOB_ERROR/python
	oLogger = logging.getLogger(__file__)
	oSvSlack = sv_slack.svSlack('dbs')
	#oLogger.debug(event.code)  # raised event code defined by APS; normal completion designates 4096 = events.EVENT_JOB_EXECUTED
	#oLogger.debug(event.exception) # raised event code defined by sys.exit(); normal completion designates None
	
	if( str(event.exception) == sv_events.EVENT_JOB_SHOULD_BE_REMOVED ):
		oLogger.debug('######_my_listener will remove job, id: ' + event.job_id + ' ##########')
		oSvSlack.sendMsg('_my_listener has removed job, id: ' + event.job_id )
		g_oScheduler.remove_job(event.job_id)
	elif( str(event.exception) == sv_events.EVENT_JOB_COMPLETED ):
		oLogger.debug('######_my_listener will remove job, id: ' + event.job_id + ' ##########')
		oSvSlack.sendMsg('_my_listener has removed job, id: ' + event.job_id )
		g_oScheduler.remove_job(event.job_id)
		oLogger.debug('######_my_listener will togle job, id: ' + event.job_id + ' from table ##########')
		oSvSlack.sendMsg('_my_listener has togled job, id: ' + event.job_id + ' from table' )
		with sv_mysql.SvMySql() as oSvMysql: # to enforce follow strict mysql connection mgmt
			aRawMySqlJobs = oSvMysql.executeQuery('updateJobIsActive', 'N', event.job_id )

	if( event.code == events.EVENT_JOB_EXECUTED or str(event.exception) == sv_events.EVENT_JOB_COMPLETED):
		_arouseDbo(event.job_id)

def _syncJobFromMysqlDb():
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
	
	oLogger = logging.getLogger(__file__)
	try:
		with sv_mysql.SvMySql() as oSvMysql: # to enforce follow strict mysql connection mgmt
			aRawMySqlJobs = oSvMysql.executeQuery('getJobList', 'Y' )
	
			# remove any job existed in Mysql but application_date is earlier than modification_date; remove updated job to add newly
			for dictMySqlJob in aRawMySqlJobs:
				if( dictMySqlJob['modification_date'] ):
					nModificationDate = int( dictMySqlJob['modification_date'].replace('-', '') )
				else:
					nModificationDate = 0

				if( dictMySqlJob['application_date'] ):
					nApplicationDate = int( dictMySqlJob['application_date'].replace('-', '') )
				else:
					nApplicationDate = 0
				
				if( nModificationDate > nApplicationDate ):
					oLogger.debug(str(dictMySqlJob['job_srl'])+' should be updated')
					try:
						g_oScheduler.remove_job(str(dictMySqlJob['job_srl']))
					except Exception as inst:
						oLogger.debug(inst) # print debug msg and ignore if job is not existed
			
			# arrange job list retrieved from Mysql
			dictJobsInMysql = {}
			for dictMySqlJob in aRawMySqlJobs:
				dictMySqlJob[str(dictMySqlJob['job_srl'])] = dictMySqlJob['job_title']
				dictJobsInMysql[str(dictMySqlJob['job_srl'])] = dictMySqlJob['job_title']
			
			dictJobs = g_oScheduler.get_jobs() 
			dicJobsInSqlite = {}
			for job in dictJobs:
				dicJobsInSqlite[job.id] = job.name
			
			# remove any job existed in Sqllite but not in Mysql
			for jobIdInSqlite in dicJobsInSqlite.keys():
				oLogger.debug( jobIdInSqlite )
				try:
					dictJobsInMysql[jobIdInSqlite]
				except KeyError:
					oLogger.debug( jobIdInSqlite + ' is not existed in dictMySqlJob - will be deleted' )
					g_oScheduler.remove_job(jobIdInSqlite)
				else:
					oLogger.debug( jobIdInSqlite + ' exists in dictMySqlJob - pass' )

			# add any job existed in Mysql but not in Sqllite
			for dictMySqlJob in dictJobsInMysql:
				try:
					dicJobsInSqlite[dictMySqlJob] 
				except KeyError: 
					oLogger.debug( dictMySqlJob + ' is not existed in jobIdInSqlite - will be added' )
					for dictRow in aRawMySqlJobs:
						if str(dictMySqlJob) == str(dictRow['job_srl']):
							sPluginName = dictRow['plugin_name']
							oSvPluginValidation = sv_plugin.svPluginValidation()
							dictValidation = oSvPluginValidation.validatePlugin( sPluginName )
							#####################
							#return
							######################
							if dictValidation['validation']: #add job if valid
								sTriggerType = dictRow['job_trigger_type']
								oTriggerParams = _parseTriggerParams(sTriggerType, dictRow['trigger_params'])
								# http://apscheduler.readthedocs.io/en/latest/modules/schedulers/base.html
								u_id = str( dictRow['job_srl'] )
								u_name =  dictRow['job_title']
								u_start_date = dictRow['start_date'] or '1970-01-01'
								u_end_date = dictRow['end_date'] or '2200-12-31'
								if( sTriggerType == 'interval' ):
									oLogger.debug( oTriggerParams )
									u_wks = int( oTriggerParams['weeks'] )
									u_days = int( oTriggerParams['days'] )
									u_hrs = int( oTriggerParams['hours'] )
									u_mins = int( oTriggerParams['minutes'] )
									u_secs = int( oTriggerParams['seconds'] )
									# http://apscheduler.readthedocs.io/en/latest/modules/triggers/interval.html
									g_oScheduler.add_job(sv_plugin.svPluginJob, 'interval', [sPluginName, dictRow['plugin_params']],
										weeks = u_wks, days = u_days, hours = u_hrs, minutes = u_mins, seconds = u_secs, 
										id = u_id, name = u_name, start_date = u_start_date, end_date = u_end_date )
								elif( sTriggerType == 'cron' ):							
									oLogger.debug( oTriggerParams )
									u_yr = oTriggerParams['year']
									u_mo = oTriggerParams['month']
									u_wk = oTriggerParams['week']
									u_dow = oTriggerParams['day_of_week']
									u_day = oTriggerParams['day']
									u_hr = oTriggerParams['hour']
									u_min = oTriggerParams['minute']
									u_sec = oTriggerParams['second']
									# http://apscheduler.readthedocs.io/en/latest/modules/triggers/cron.html
									g_oScheduler.add_job(sv_plugin.svPluginJob, 'cron', [sPluginName, dictRow['plugin_params']],
										year = u_yr, month = u_mo, week = u_wk, day_of_week = u_dow, day = u_day, hour = u_hr, minute = u_min, second = u_sec,
										id = u_id, name = u_name, start_date = u_start_date, end_date = u_end_date )
														
								sCurDatetime = datetime.today().strftime("%Y%m%d%H%M%S")
								oSvMysql.executeQuery('updateJobAppliedDate', sCurDatetime, dictRow['job_srl'] )
								oLogger.debug( u_name + ' with ' + sTriggerType + ' trigger has been added' )
							else:
								oLogger.debug("plugin '%s' not implemented" % sPluginName)
				else:
					oLogger.debug( dictMySqlJob + ' exists in jobIdInSqlite - pass' )
	except IOError as err:
		oLogger.debug( err )
		raise Exception( err) # exit immediately with exit code 127

def _parseTriggerParams(sTriggerType, sJson):
	lstParams = dict()
	_tmpLstParams = json.loads(sJson)
	if( sTriggerType == 'interval' ):
		dictTriggerParamMapping = {'interval_wk':'weeks', 'interval_day':'days', 'interval_hour':'hours', 'interval_min':'minutes', 'interval_sec':'seconds'}
	elif( sTriggerType == 'cron' ):
		dictTriggerParamMapping = {'cron_year':'year', 'cron_month':'month', 'cron_week':'week', 'cron_day':'day', 'cron_day_of_week':'day_of_week', 'cron_hour':'hour', 'cron_minute':'minute', 'cron_second':'second'}

	for key, value in dictTriggerParamMapping.items():
		try:
			_tmpLstParams[key]
		except KeyError as inst:
			if( sTriggerType == 'interval' ):
				lstParams[value] = 0
			elif( sTriggerType == 'cron' ):
				lstParams[value] = None			
		else: 
			lstParams[value] = _tmpLstParams[key]
	return lstParams

if __name__ == '__main__':
	main()