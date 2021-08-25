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
import base64
import hashlib
from Cryptodome.Cipher import AES  # pip3.8 install pycryptodomex


class svCipherOpenSsl:
    """ 
    compatible with php openssl_encrypt()
    this class can be utilzied between python-php secured communcation
    refer to https://github.com/arajapandi/php-python-encrypt-decrypt
    """
    # rawkey = 'asdfa923aksadsYahoasdw998sdsads'
    method = AES.MODE_CFB
    blocksize = 32  # 16, 32..etc
    padwith = '`'  # padding value for string
    __g_oLogger = None
    iv = None
    key = None
   
    def __init__(self):
        self.__g_oLogger = logging.getLogger(__file__)

#    def __init__(self, iv, key=''):
#        """
#        construct for cypher class - get, set key and iv
#        """
#        if not key:
#            key = self.rawkey
#
#        self.key = key
#        self.iv = iv

    # lambda function for padding
    def pad(self, s): return s + (self.blocksize - len(s) % self.blocksize) * bytes(self.padwith, 'utf-8')

    def setIv( self, sIv ):
        self.iv = sIv
    
    def setSecretKey( self, sSecretKey ):
        self.key = sSecretKey

    def encryptString(self, raw):
        """Encrypt given string using AES encryption standard"""
        cipher = AES.new(self.__getKEY(), self.method, self.__getIV(), segment_size=128)
        return base64.b64encode(cipher.encrypt(self.pad(raw.encode('utf8'))))

    def decryptString(self, encrypted):
        """Decrypt given string using AES standard"""
        encrypted = base64.b64decode(encrypted)
        cipher = AES.new(self.__getKEY(), self.method, self.__getIV(), segment_size=128)
        return cipher.decrypt(encrypted).decode('utf-8').rstrip(self.padwith)

    def __getKEY(self):
        """get hashed key - if key is not set on init, then default key wil be used"""
        if not self.key:
            self.__printDebug('hashed key not exists')
            return False
        else:
            # print(self.key)
            return bytes(hashlib.sha256(self.key.encode('utf-8')).hexdigest()[:32], 'utf-8')

    def __getIV(self):
        """ get hashed IV value - if no IV values then it throw error"""
        if not self.iv:
            self.__printDebug('hashed IV not exists')
            return False
        else:
            return bytes(hashlib.sha256(self.iv.encode('utf-8')).hexdigest()[:16], 'utf-8')

    def __printDebug( self, sMsg ):
        if __name__ == '__main__' or __name__ == 'sv_cipher':
            print( sMsg )
        else:
            if( self.__g_oLogger is not None ):
                self.__g_oLogger.debug( sMsg )


class svCipherMcrypt:
    ''' 
    compatible with php mcrypt_module_open()
    but php mcrypt has been deprecated since php 7.2
    this class can be utilzied by python-internal encryption and decryption only
    # https://gist.github.com/jeruyyap/8cc375a1d50abdf234e7
    '''
    __g_oLogger = None
    __g_sSecret = None
    __g_sIv = None
    __g_sPadding = '{'

    def __init__(self):
        self.__g_oLogger = logging.getLogger(__file__)

    def setIv( self, sIv ):
        self.__g_sIv = sIv
    
    def setSecretKey( self, sSecretKey ):
        self.__g_sSecret = sSecretKey

    def decryptString( self, sTarget ):
        #self.__printDebug(self.__g_sPadding.encode('utf8') )
        #DecodeAES = lambda c, e: c.decrypt(base64.b64decode(e)).rstrip(self.__g_sPadding.encode('utf8'))
        DecodeAES = lambda c, e: c.decrypt(base64.b64decode(e)).rstrip('\x00'.encode('utf8'))
        cipher = AES.new(key = self.__g_sSecret.encode('utf8'),mode=AES.MODE_CBC,IV = self.__g_sIv.encode('utf8'))
        return DecodeAES(cipher, sTarget)

    def encryptString( self, sTarget ):
        # conversion between str and byte
        # https://stackoverflow.com/questions/50302827/object-type-class-str-cannot-be-passed-to-c-code-virtual-environment
        # encrypt transmission between php and python
        # https://stackoverflow.com/questions/13051293/encrypt-data-with-python-decrypt-in-php

        # the block size for the cipher object; must be 16, 24, or 32 for AES
        BLOCK_SIZE = 32
        # the character used for self.__g_sPadding--with a block cipher such as AES, the value
        # you encrypt must be a multiple of BLOCK_SIZE in length.  This character is
        # used to ensure that your value is always a multiple of BLOCK_SIZE

        # one-liner to sufficiently pad the text to be encrypted
        pad = lambda s: s + (BLOCK_SIZE - len(s) % BLOCK_SIZE) * self.__g_sPadding.encode('utf8')

        # one-liners to encrypt/encode and decrypt/decode a string
        # encrypt with AES, encode with base64
        EncodeAES = lambda c, s: base64.b64encode(c.encrypt(pad(s)))
        cipher = AES.new(key = self.__g_sSecret.encode('utf8'),mode=AES.MODE_CBC,IV = self.__g_sIv.encode('utf8'))
        return EncodeAES(cipher, sTarget.encode('utf8'))

    def __printDebug( self, sMsg ):
        if __name__ == '__main__' or __name__ == 'sv_cipher':
            print( sMsg )
        else:
            if( self.__g_oLogger is not None ):
                self.__g_oLogger.debug( sMsg )

if __name__ == '__main__': # for console debugging
    secret = 'TestString From PHP345'
    print(secret)
    sv_secret_key = '34yuhangencer4ty'
    sv_iv = 'HyuhangenDdfe5gh'

    oSvCipher = svCipherOpenSsl()
    oSvCipher.setIv(sv_iv)
    oSvCipher.setSecretKey(sv_secret_key)

    s_enc = oSvCipher.encryptString(secret).decode("utf-8")
    print(s_enc)

    print(oSvCipher.decryptString(s_enc.encode("utf-8")))
#    oSvCipher = svCipher()
#    sv_secret_key = '34yuhanroxc13234'
#    sv_iv = 'HyuhanroxD567856'
#    oSvCipher.setIv(sv_iv)
#    oSvCipher.setSecretKey(sv_secret_key)
#
#    enc_text = oSvCipher.encryptString('test message')
#    print(enc_text)
#    dec_text = oSvCipher.decryptString(enc_text)
#    print(dec_text)
