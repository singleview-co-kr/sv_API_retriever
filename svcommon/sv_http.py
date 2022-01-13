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
import base64
import http.client # https://docs.python.org/3/library/http.client.html
import urllib.parse
import logging
import re # https://docs.python.org/3/library/re.html
import json
# from Cryptodome.Cipher import AES

# 3rd party library
import simplejson as json

if __name__ == 'svcommon.sv_http': # for platform running
    from svcommon import sv_object
    from svcommon import sv_cipher
elif __name__ == 'sv_http': # for plugin console debugging
    import sv_object
    import sv_cipher
elif __name__ == '__main__': # for class console debugging
    sys.path.append('../svcommon')
    import sv_object
    import sv_cipher


class svHttpCom(sv_object.ISvObject):
    """ HTTP communication class for singleview only """
    __g_oHttpConn = None
    #__g_oLogger = None
    __g_oCipher = None
    __g_sSubUrl = None
    __g_sReservedQueryName = '@v'
    __g_dictRet = {'error': -1, 'variables':{'todo':'remove'}} # simulate XE object respond
    __g_dictMsg = { 
        'OK': 1, # OK
        'FIN': 2, # finish 
        'MIHY': 3, # may i help you?
        'LMKL': 4, # let me know new data
        'IWSY': 5, # I will send you
        'ALD': 6, # add latest data
        # 'MTG': 7, # more to go
        # 'IHND': 8, # i have new data
        'IWWFY': 9, # i will wait for you
        'IHNI': 10, # i have no idea
        # 'RRC': 11, # remaining record count
        'PUP': 12, # Plz Update Period
        'LMKP': 13, # Let me know Period
        'WLYK': 14, # will Let you know
        # 'LMKLD': 1, # let me know latest data + data: requested sync date since
    	# 'FIN': 2, # finish 	
    	# 'ALD': 3, # add latest data + data: doc_srls + com_srls
	    'GMDL': 15, # give me document list  -> data: doc_srls
    	'GMCL': 16, # give me comment list  -> data: com_srls
    	'HYA': 17, # here you are -> data: text list
        }

    def __init__(self, sFullUrl):
        o_port_regex = re.compile(r":(\d+)") 
        m_port = o_port_regex.search(sFullUrl)
        s_port_number = ''
        if m_port is not None:
            s_port_number = m_port.group(0)  # s_port_number is ':XXXX'
            del m_port
        del o_port_regex
        
        o_server_name_regex = re.compile(r"https?:\/\/[\w\-.]+")
        o_server_name = o_server_name_regex.search(sFullUrl)
        if o_server_name is not None:
            s_host_name = o_server_name.group(0)
            if 'http://' in s_host_name:
                s_host_name = s_host_name.replace('http://', '')
                self.__g_oHttpConn = http.client.HTTPConnection(s_host_name + s_port_number)
                self.__g_sSubUrl = sFullUrl.replace('http://' + s_host_name + s_port_number, '')
            elif 'https://' in s_host_name:
                s_host_name = s_host_name.replace('https://', '')
                self.__g_oHttpConn = http.client.HTTPSConnection(s_host_name + s_port_number)
                self.__g_sSubUrl = sFullUrl.replace('https://' + s_host_name + s_port_number, '')
            del o_server_name
        del o_server_name_regex

        self._g_oLogger = logging.getLogger(__file__)
        self.__g_oCipher = sv_cipher.svCipherOpenSsl()  # sv_cipher.svCipherMcrypt()

    def getSecuredUrl(self, dictParams):
        # oResp = None
        # refer to https://velog.io/@city7310/%ED%8C%8C%EC%9D%B4%EC%8D%AC%EC%9C%BC%EB%A1%9C-URL-%EA%B0%80%EC%A7%80%EA%B3%A0-%EB%86%80%EA%B8%B0
        oOriginalParts = urllib.parse.urlparse(self.__g_sSubUrl)
        #oOriginalParts.scheme == 'https'
        #oOriginalParts.netloc == 'velog.io:80'
        #oOriginalParts.path == '/tags/'
        #oOriginalParts.params == ''
        #oOriginalParts.query == 'sort=name&dd=33'
        #oOriginalParts.fragment == ''
        if len(oOriginalParts.query) > 0:
            dictQuery = json.dumps(urllib.parse.parse_qs(oOriginalParts.query))
        
        self.__g_oCipher.setIv(dictParams.pop('iv'))
        self.__g_oCipher.setSecretKey(dictParams.pop('secret'))
        sJson = self.__g_oCipher.encryptString(dictQuery)
        sEncryptedJsonQuery = sJson.decode('utf-8')
        sEncodedEncryptedJsonQuery = urllib.parse.urlencode({self.__g_sReservedQueryName:sEncryptedJsonQuery})
        oDecryptedParts = urllib.parse.ParseResult(scheme=oOriginalParts.scheme, netloc=oOriginalParts.netloc, path=oOriginalParts.path, params='', query=sEncodedEncryptedJsonQuery, fragment='')
        sConvertedUrl = urllib.parse.urlunparse(oDecryptedParts) # or oDecryptedParts.geturl()
        try:
            sBody = '{}'
            dictHeaders = {
                'Content-Type': 'application/json',
                'User-Agent' : 'sv_crontab_bot' # to deny illegal access ;; 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.101 Safari/537.36'
            }
            self.__g_oHttpConn.request('GET', sConvertedUrl, sBody, dictHeaders)
            oHttpResp = self.__g_oHttpConn.getresponse()
            if oHttpResp.status == 200 and oHttpResp.reason == 'OK':
                sResp = oHttpResp.read().decode('utf-8')   # This will return entire content.
                #self._printDebug(sResp)
                if sResp is not 'NULL':
                    sTmp = self.__g_oCipher.decryptString(sResp)
                    oTmp = json.loads(sTmp)
                self.__g_dictRet['error'] = 0
                self.__g_dictRet['variables'] = oTmp
            else:
                pass # what if HTTP failed
        except Exception as err:  
            nIdx = 0
            for e in err.args:
                self._printDebug('http generic error raised arg' + str(nIdx) + ': ' + str(e))
                nIdx += 1
            # oResp = self.__g_dictRet
        finally:
            return self.__g_dictRet
        
    def getUrl(self):
        oResp = None
        try:
            self.__g_oHttpConn.request('GET', self.__g_sSubUrl)
            oHttpResp = self.__g_oHttpConn.getresponse()
            if oHttpResp.status == 200 and oHttpResp.reason == 'OK':
                sResp = oHttpResp.read().decode('utf-8')   # This will return entire content.
                # self._printDebug(sResp)
                oResp = json.loads(base64.b64decode(sResp)) # php XE::Object will not be received
            else:
                pass # what if HTTP failed
        except Exception as err:  
            nIdx = 0
            for e in err.args:
                self._printDebug('http generic error raised arg' + str(nIdx) + ': ' + str(e))
                nIdx += 1
            oResp = self.__g_dictRet
        finally:
            return oResp # should be changed to self.__g_dictRet

    def postUrl(self, dictParams):
        try:
            if isinstance(dictParams, dict):
                self.__g_oCipher.setIv(dictParams.pop('iv'))
                self.__g_oCipher.setSecretKey(dictParams.pop('secret'))
                sJson = json.dumps(dictParams)
                #self._printDebug(sJson)
                sJson = self.__g_oCipher.encryptString(sJson)
                #self._printDebug(sJson)
                oHeaders = {'Content-type': 'application/x-www-form-urlencoded','Accept': 'application/json'}
                sParams = urllib.parse.urlencode({self.__g_sReservedQueryName: sJson})
                # self._printDebug(self.__g_sSubUrl)
                self.__g_oHttpConn.request('POST', self.__g_sSubUrl, sParams, oHeaders)
                oHttpResp = self.__g_oHttpConn.getresponse()
                if oHttpResp.status == 200 and oHttpResp.reason == 'OK':
                    sResp = oHttpResp.read().decode('utf-8')   # This will return entire content.
                    # self._printDebug(sResp)
                    if sResp == 'bar1':
                        self._printDebug('invalid brand_id')
                        self.__g_dictRet['error'] = -1
                        self.__g_dictRet['variables']['todo'] = 'stop'
                    elif sResp == 'bar2':
                        self._printDebug('enc key not exist')
                        self.__g_dictRet['error'] = -1
                        self.__g_dictRet['variables']['todo'] = 'stop'
                    elif sResp == 'bar3':
                        self._printDebug('decryption failed')
                        self.__g_dictRet['error'] = -1
                        self.__g_dictRet['variables']['todo'] = 'stop'
                    elif sResp == 'bar4':
                        self._printDebug('not a debug mode')
                        self.__g_dictRet['error'] = -1
                        self.__g_dictRet['variables']['todo'] = 'stop'
                    elif sResp != 'NULL':
                        sTmp = self.__g_oCipher.decryptString(sResp)
                        oTmp = json.loads(sTmp )
                        #self._printDebug('' )
                        #self._printDebug(oTmp )
                        #self._printDebug('' )
                        self.__g_dictRet['error'] = 0
                        self.__g_dictRet['variables'] = oTmp
                    #while not r1.closed:
                    #	print(r1.read(200))  # 200 bytes
                else: # what if HTTP failed
                    self._printDebug('invalid URL -> status:' + str(oHttpResp.status) + ' reason:' + oHttpResp.reason)
            else:
                self._printDebug('not a dict type params')

        except Exception as err:
            nIdx = 0
            for e in err.args:
                self._printDebug('http generic error raised arg' + str(nIdx) + ': ' + str(e))
                nIdx += 1
        finally:
            return self.__g_dictRet
    
    def getMsgDict(self):
        return self.__g_dictMsg

    def close(self):
        if self.__g_oHttpConn is not None:
            self.__g_oHttpConn.close()


if __name__ == '__main__': # for console debugging
    oSvHttpCom = svHttpCom('localhost')
    dictParams = {'@key': 234, '@type': 'issue', '@action': 'show'}
    oResp = oSvHttpCom.postUrl('/devel/api/', dictParams)
    oSvHttpCom.close()
    #print(oResp)

# The following example demonstrates reading data in chunks.
'''oHttpConn.request("GET", "/")
'''
