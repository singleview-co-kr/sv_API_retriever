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

# doing by schedule bot
# singleview config
from conf import basic_config

# standard library
import sys
import time
import datetime
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

g_oScheduler = None
g_sVersion = '0.0.1'
g_sLastModifiedDate = '18th, Feb 2018'

class setSvDaemon(daemonocle.Daemon):
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
		print( 'doing-by-order bot all rights reserved by singleview.co.kr' )
		print( 'Version: '+ g_sVersion + ' modified on ' + g_sLastModifiedDate )
	
	def __getLogger(self):
		""" this class seems to be created by different process with main(), that is configuration run by main() is not working in this code block 
			main() is not executed even if you run the CLI [python3.6 crawler.py banana] """
		logging.config.fileConfig( basic_config.ABSOLUTE_PATH_BOT+'/conf/logging_dbo_exposure_action.conf') # https://docs.python.org/3.3/library/logging.config.html
		return logging.getLogger('exposure_action')

def cb_shutdown(message, code):
	logging.info(__file__ + ' v' + g_sVersion + ' has been shutdown')
	logging.debug(message)

@click.command(cls=DaemonCLI, daemon_class=setSvDaemon, daemon_params={'shutdown_callback': cb_shutdown, 'pidfile': './misc/dbo.pid'})
def main():
	""" This is singleview doing-by-schedule bot. It assists to run various regular job."""
	#global g_oScheduler
	#g_oScheduler.pause() #g_oScheduler.resume()

	# create logger
	logging.config.fileConfig( basic_config.ABSOLUTE_PATH_BOT+'/conf/logging_dbo.conf') # https://docs.python.org/3.3/library/logging.config.html
	oLogger = logging.getLogger(__file__)
	oLogger.info(__file__ + ' v' + g_sVersion + ' has been started')
	
	# Register the signal handlers
	signal.signal(signal.SIGUSR1, _receiveSignal)

	_startScheduler()
	
	while True:  # 'daemon' code
		time.sleep(6) # process CPU shr % will go up to 100% w/o this execution

def _startScheduler():
	global g_oScheduler
	# The "apscheduler." prefix is hard coded
	# need to install sqlalchemy module first refer to http://docs.sqlalchemy.org/en/latest/intro.html
	g_oScheduler = BackgroundScheduler({
			'apscheduler.executors.default': {
				'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
				'max_workers': '40'
			},
			'apscheduler.job_defaults.coalesce': 'false',
			'apscheduler.job_defaults.max_instances': '10',
			'apscheduler.timezone': 'Asia/Seoul', 
		})
	g_oScheduler.add_listener(_my_listener, events.EVENT_ALL ) #events.EVENT_JOB_EXECUTED | events.EVENT_JOB_ERROR)
	g_oScheduler.start()

def _my_listener(event):
	'''Listens to completed job'''
	# http://apscheduler.readthedocs.io/en/latest/modules/events.html
	# http://nullege.com/codes/show/src%40c%40k%40ckan-service-provider-HEAD%40ckanserviceprovider%40web.py/69/apscheduler.events.EVENT_JOB_ERROR/python
	oLogger = logging.getLogger(__file__)
	#oLogger.debug('########event.code: ' + str( event.code ))  # raised event code defined by APS; normal completion designates 4096 = events.EVENT_JOB_EXECUTED
	#oLogger.debug(event.exception) # raised event code defined by sys.exit(); normal completion designates None
	
	if( event.code == events.EVENT_JOB_EXECUTED ):
		oLogger.debug('######_my_listener will remove job, id: ' + event.job_id + ' ##########')
		#g_oScheduler.remove_job(event.job_id)
		_recieveTrigger(event.job_id)

def _recieveTrigger(sJobId):
	oLogger = logging.getLogger(__file__)
	oLogger.debug('_recieveTrigger')
	oLogger.debug(sJobId)	
	if sJobId.find('_') != -1:
		oLogger.debug('reached end of job')
	else:
		oLogger.debug('go recursive!')
		with sv_mysql.SvMySql() as oSvMysql: # to enforce follow strict mysql connection mgmt
			lstHandovers = oSvMysql.executeQuery('getDboHandoverBySrl', sJobId )
			nRootJobId = lstHandovers[0]['caller_dbo_job_srl']
			try:
				sCallerPluginName = lstHandovers[0]['caller_plugin_name']
			except IndexError:
				return

			sId = str( lstHandovers[0]['handover_srl'] )			
			_addHandoverJob(sCallerPluginName, sId, lstHandovers[0]['caller_plugin_params'])
			# toggle off touched handover job
			nHandoverJobSrl = lstHandovers[0]['handover_srl']
			oSvMysql.executeQuery('updateHandoverDoneBySrl', 'Y', nHandoverJobSrl )
	
	
def _receiveSignal(signum, stack):
	oLogger = logging.getLogger(__file__)
	oLogger.info( '_receiveSignal Received:' + str( signum ) + ' @ ' + str(time.ctime()))
	with sv_mysql.SvMySql() as oSvMysql: # to enforce follow strict mysql connection mgmt
		lstHandovers = oSvMysql.executeQuery('getActiveHandover' )
		for dictHandover in lstHandovers:
			nRootJobId = dictHandover['dbs_job_srl']
			try:
				sCallerPluginName = dictHandover['caller_plugin_name']
			except IndexError:
				continue			

			sId = str( dictHandover['handover_srl'] )			
			_addHandoverJob(sCallerPluginName, sId, dictHandover['caller_plugin_params'])
			# toggle off touched handover job
			nHandoverJobSrl = dictHandover['handover_srl']
			oSvMysql.executeQuery('updateHandoverDoneBySrl', 'Y', nHandoverJobSrl )

def _addHandoverJob(sCallerPluginName, sId, sJsonCallerPluginParams):
	#dictPluginMap = {'blank': 'nvad_register_db','nvad_register_db': 'ga_register_db'}
	oLogger = logging.getLogger(__file__)
	
	# check if caller plugin is valid
	oSvPluginValidation = sv_plugin.svPluginValidation()
	dictCallerValidation = oSvPluginValidation.validatePlugin( sCallerPluginName )
	if dictCallerValidation['validation']:
		lstFollowupJobPluginName = dictCallerValidation['followup_job']
		#oLogger.debug(lstFollowupJobPluginName)				
		
		if lstFollowupJobPluginName[0] is not 'no':
			bValidFollwupJobPlugin = True
			for sFollowupJobPluginName in lstFollowupJobPluginName:
				#oLogger.debug(sFollowupJobPluginName)
				dictFollowupJobValidation = oSvPluginValidation.validatePlugin( sFollowupJobPluginName )
				# check if followup plugin is valid
				if dictFollowupJobValidation['validation'] is False:
					#oLogger.debug(dictFollowupJobValidation)
					bValidFollwupJobPlugin = False
			
			if bValidFollwupJobPlugin:
				nIdx = 0
				for sFollowupJobPluginName in lstFollowupJobPluginName:
					# find handover job plugin based on call job plugin
					dictPluginParams = json.loads(sJsonCallerPluginParams)
					#oLogger.info('_addHandoverJob caller job plugin name: ' + dictPluginParams['plugin_name'])
					#oLogger.info('_addHandoverJob handover job plugin name: ' + sFollowupJobPluginName )
					dictFollowFollowupJobValidation = oSvPluginValidation.validatePlugin( sFollowupJobPluginName )
					
					if dictFollowFollowupJobValidation['followup_job'][0] is 'no': 
						sJobId = sId + '_' + str( nIdx )
					else:
						with sv_mysql.SvMySql() as oSvMysql: 
							sCurDatetimeStamp = time.strftime('%Y%m%d%H%M%S')
							lstRst = oSvMysql.executeQuery('insertHandoverOnDbo', sId, sFollowupJobPluginName, sJsonCallerPluginParams, sCurDatetimeStamp )
							sJobId = str( lstRst[0]['id'])
										
					oLogger.debug('################')
					oLogger.debug(dictFollowFollowupJobValidation['followup_job'][0])
					oLogger.debug(sJobId)
					oLogger.debug('################')
					sJobName =  sJobId + '_' + sFollowupJobPluginName + ' by ' + dictPluginParams['plugin_name']
					dictPluginParams['plugin_name'] = sFollowupJobPluginName
					
					dtNow = datetime.datetime.now()
					dtRunTiming = dtNow + datetime.timedelta(seconds=3)
					u_yr = dtRunTiming.strftime('%Y')
					u_mo = dtRunTiming.strftime('%m')
					u_day = dtRunTiming.strftime('%d')
					u_hr = dtRunTiming.strftime('%H')
					u_min = dtRunTiming.strftime('%M')
					u_sec = dtRunTiming.strftime('%S')
					
					g_oScheduler.add_job(sv_plugin.svPluginJob, 'cron', [sFollowupJobPluginName, dictPluginParams], 
						year = u_yr, month = u_mo, day = u_day, hour = u_hr, minute = u_min, second = u_sec, id = sJobId, name = sJobName )
					nIdx += 1
					
					
			else:
				#g_oScheduler.remove_job(nParentJobId)
				oLogger.debug('######_arouseDbo has removed job, id: ' + nParentJobId + ' as its followup.conf contains wrong definition ##########')
		else:
			oLogger.debug('no follow up')

if __name__ == '__main__':
	main()