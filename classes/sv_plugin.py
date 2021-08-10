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

# singleview config
from conf import basic_config

# standard library
import sys
import os
from os import listdir
import importlib
import time

import logging
import re # https://docs.python.org/3/library/re.html

# 3rd party library
import simplejson as json

# singleview library
from classes import sv_events

class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class SvErrorHandler(Error):
	"""Raised when the http ['variables']['todo'] is set """
	__g_oLogger = None

	def __init__(self, sTodo):
		self.__g_oLogger = logging.getLogger(__file__)
		#self.__printDebug(sTodo)
		if( sTodo == 'stop' ):
			self.__printDebug('should stop job and wait next schedule')
			sys.exit(sv_events.EVENT_JOB_SHOULD_BE_STOPPED) # sys.exit() signal in sv_http module does not reach to scheduler, as this module is not called directly
		elif( sTodo == 'wait' ): # might be removed????
			self.__printDebug('wait 3 seconds till problem solved')
			time.sleep(3)
		elif( sTodo == 'remove' ):
			self.__printDebug('remove job immediately')
			sys.exit(sv_events.EVENT_JOB_SHOULD_BE_REMOVED) # raise event code to remove job if the connected method is invalid
		elif( sTodo == 'completed' ):
			self.__printDebug('remove and toggle job table')
			sys.exit(sv_events.EVENT_JOB_COMPLETED) # raise event code to remove job if the connected method is invalid
		elif( sTodo == 'time_sync' ):
			self.__printDebug('stop scheduler')
		else:
			self.__printDebug('general error occured')

	def __printDebug( self, sMsg ):
		if __name__ == '__main__':
			print( sMsg )
		else:
			if( self.__g_oLogger is not None ):
				self.__g_oLogger.debug( sMsg )

class svPluginJob():
	__g_oLogger = None

	def __init__(self, *lstPluginParams):
		# https://docs.python.org/3.6/library/importlib.html
		self.__g_oLogger = logging.getLogger(__file__)
		try:
			dictPluginParams = json.loads( lstPluginParams[1] )
		except TypeError:
			dictPluginParams = lstPluginParams[1]
		
		sPluginTitle = lstPluginParams[0]
		sPluginNameInParam = dictPluginParams.pop('plugin_name', None)

		if( sPluginNameInParam != sPluginTitle ):
			self.__printDebug('plugin param is weird -> remove job')
			raise SvErrorHandler('remove') # raise event code to remove job if the connected method is invalid
		
		try:
			oJobPlugin = importlib.import_module('job_plugins.' + sPluginTitle + '.task')
			with oJobPlugin.svJobPlugin(dictPluginParams) as oJob: # to enforce each plugin follow strict guideline or remove from scheduler
				self.__printDebug( oJob.__class__.__name__ + ' has been initiated' )
				oJob.procTask()
		except AttributeError: # if task module does not have svJobPlugin
			self.__printDebug('plugin does not have correct method -> remove job')
			raise SvErrorHandler('remove')
		except ModuleNotFoundError:
			self.__printDebug('plugin is not existed -> remove job')
			raise SvErrorHandler('remove')
		except Exception as err:
			nIdx = 0
			for e in err.args:
				if( e == 'stop' or e == 'wait' ): # handle HTTP err response from XE
					self.__printDebug('handle HTTP err response from XE: ' + e)
					raise SvErrorHandler(e)
				elif( e == 'completed' ): # handle completed exception signal from each job
					self.__printDebug('raised completed job!')
					raise SvErrorHandler(e)
				try:
					self.__printDebug('plugin occured general exception arg' + str(nIdx) + ': ' + e)
				except TypeError:
					self.__printDebug('plugin occured general exception arg' + str(nIdx) + ': ')
					self.__printDebug( e)
				nIdx += 1
			raise SvErrorHandler('remove')
		finally:
			pass
	
	def __printDebug( self, sMsg ):
		if __name__ == '__main__' or __name__ == 'sv_plugin':
			print( sMsg )
		else:
			if( self.__g_oLogger is not None ):
				self.__g_oLogger.debug( sMsg )

class svPluginValidation():
	__g_oLogger = None
	__g_sPluginTitle = None

	def __init__(self):
		self.__g_oLogger = logging.getLogger(__file__)
		
	def __printDebug( self, sMsg ):
		if __name__ == '__main__':
			print( sMsg )
		else:
			if( self.__g_oLogger is not None ):
				self.__g_oLogger.debug( sMsg )

	def __parseFolloup( self, sStr ):
		lstFollowup = list()
		regex = r"(#[^\r\n]*)"
		matches = re.finditer(regex, sStr)
		for matchNum, match in enumerate(matches):
			matchNum = matchNum + 1			
			#self.__printDebug ("Match {matchNum} was found at {start}-{end}: {match}".format(matchNum = matchNum, start = match.start(), end = match.end(), match = match.group()))
			for groupNum in range(0, len(match.groups())):
				groupNum = groupNum + 1
				#self.__printDebug ("Group {groupNum} found at {start}-{end}: {group}".format(groupNum = groupNum, start = match.start(groupNum), end = match.end(groupNum), group = match.group(groupNum)))
				sStr = re.sub(match.group(groupNum), '', sStr )
		# Note: for Python 2.7 compatibility, use ur"" to prefix the regex and u"" to prefix the test string and substitution.
		for line in sStr.splitlines():
			if line:
				lstFollowup.append(line.strip())
		
		return lstFollowup
	
	def validatePlugin(self, sPluginName):
		""" find the module directory starts its name with "job_" in /job_plugins folder """
		self.__g_sPluginTitle = sPluginName
		sPluginFolderName = self.__g_sPluginTitle
		sJobPluginPath = basic_config.ABSOLUTE_PATH_BOT+'/job_plugins'
		lstJobPlugin = listdir(sJobPluginPath)
		
		dictValidation = {'validation':False, 'followup_job':['no']}
		if sPluginFolderName in lstJobPlugin:
			dictValidation['validation'] = True
			try:
				with open (sJobPluginPath + '/' + sPluginFolderName + '/followup.conf', 'r') as myfile:
					sFollowupJob = myfile.read()
					lstFollowupJob = self.__parseFolloup( sFollowupJob )
					if( len( lstFollowupJob ) ):
						dictValidation['followup_job'] = lstFollowupJob
					#nPid = int(sFollowupJob)
			except FileNotFoundError:
				#self.__printDebug('no FollowupJob')
				pass
			#return True
		#else:
		#	return None
		#self.__printDebug(dictValidation)
		return dictValidation
		#for file in listdir(sJobPluginPath):
		#	if file.endswith('.py'):
		#		self.__printDebug(file)

if __name__ == '__main__': # for console debugging
	pass

'''
def _convertString2Method( sFunctionName ):
	# https://stackoverflow.com/questions/7936572/python-call-a-function-from-string-name 
	if not len( sFunctionName ):
		return None
	possibles = globals().copy()
	possibles.update(locals())
	oMethod = possibles.get('_'+sFunctionName)
	if not oMethod:
		return None
	return oMethod
def __validateExcutableMethod(self, sMethodName):
		""" excute the method starts its name with "job_" and defined as private method only """
		oRegEx = re.compile(r"[j][o][b][_]\w+") 
		m = oRegEx.search(sMethodName)
		if( m is not None ):
			if( hasattr( self, '_'+self.__class__.__name__+'__'+sMethodName ) ): 
				return True
		return None
def doMethod(self, sMethodName, *oMethodParams):
	if( self.__validateExcutableMethod(sMethodName) ):
		try:
			oMethod = getattr(self, '_' + self.__class__.__name__ + '__'+sMethodName)  # https://stackoverflow.com/questions/7936572/python-call-a-function-from-string-name 
			oMethod( oMethodParams[0], oMethodParams[1] )
		except AttributeError:
			sys.exit(sv_events.EVENT_JOB_SHOULD_BE_REMOVED) # raise event code to remove job if the connected method is invalid'''