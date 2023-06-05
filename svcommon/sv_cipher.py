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
import logging
import base64
import hashlib
from Cryptodome.Cipher import AES  # pip3.8 install pycryptodomex

if __name__ == 'svcommon.sv_cipher':  # for platform running
    from svcommon import sv_object
elif __name__ == 'sv_cipher':  # for plugin console debugging
    import sv_object
elif __name__ == '__main__':  # for class console debugging
    sys.path.append('../svcommon')
    import sv_object


class SvCipherOpenSsl(sv_object.ISvObject):
    """ 
    compatible with php openssl_encrypt()
    this class can be utilzied between python-php secured communcation
    refer to https://github.com/arajapandi/php-python-encrypt-decrypt
    """
    __method = AES.MODE_CFB
    __n_block_size = 32  # 16, 32..etc
    __s_pad_with = '`'  # padding value for string
    # __g_oLogger = None
    iv = None
    key = None

    def __init__(self):
        self._g_oLogger = logging.getLogger(__file__)

    # lambda function for padding
    def pad(self, s):
        return s + (self.__n_block_size - len(s) % self.__n_block_size) * bytes(self.__s_pad_with, 'utf-8')

    def set_iv(self, s_iv):
        self.iv = s_iv

    def set_secret_key(self, s_secret_key):
        self.key = s_secret_key

    def encrypt_str(self, raw):
        """Encrypt given string using AES encryption standard"""
        cipher = AES.new(self.__get_key(), self.__method, self.__get_iv(), segment_size=128)
        return base64.b64encode(cipher.encrypt(self.pad(raw.encode('utf8'))))

    def decrypt_str(self, encrypted):
        """Decrypt given string using AES standard"""
        encrypted = base64.b64decode(encrypted)
        cipher = AES.new(self.__get_key(), self.__method, self.__get_iv(), segment_size=128)
        return cipher.decrypt(encrypted).decode('utf-8').rstrip(self.__s_pad_with)

    def __get_key(self):
        """get hashed key - if key is not set on init, then default key wil be used"""
        if not self.key:
            self._printDebug('hashed key not exists')
            return False
        else:
            # print(self.key)
            return bytes(hashlib.sha256(self.key.encode('utf-8')).hexdigest()[:32], 'utf-8')

    def __get_iv(self):
        """ get hashed IV value - if no IV values then it throw error"""
        if not self.iv:
            self._printDebug('hashed IV not exists')
            return False
        else:
            return bytes(hashlib.sha256(self.iv.encode('utf-8')).hexdigest()[:16], 'utf-8')


class SvCipherMcrypt(sv_object.ISvObject):
    """
    compatible with php mcrypt_module_open()
    but php mcrypt has been deprecated since php 7.2
    this class can be utilzied by python-internal encryption and decryption only
    # https://gist.github.com/jeruyyap/8cc375a1d50abdf234e7
    """
    # __g_oLogger = None
    __g_sSecret = None
    __g_sIv = None
    __g_sPadding = '{'

    def __init__(self):
        self._g_oLogger = logging.getLogger(__file__)

    def set_iv(self, s_iv):
        self.__g_sIv = s_iv

    def set_secret_key(self, s_secret_key):
        self.__g_sSecret = s_secret_key

    def decrypt_str(self, s_target):
        # self.__printDebug(self.__g_sPadding.encode('utf8') )
        # DecodeAES = lambda c, e: c.decrypt(base64.b64decode(e)).rstrip(self.__g_sPadding.encode('utf8'))
        DecodeAES = lambda c, e: c.decrypt(base64.b64decode(e)).rstrip('\x00'.encode('utf8'))
        cipher = AES.new(key=self.__g_sSecret.encode('utf8'), mode=AES.MODE_CBC, IV=self.__g_sIv.encode('utf8'))
        return DecodeAES(cipher, s_target)

    def encrypt_str(self, sTarget):
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
        cipher = AES.new(key=self.__g_sSecret.encode('utf8'), mode=AES.MODE_CBC, IV=self.__g_sIv.encode('utf8'))
        return EncodeAES(cipher, sTarget.encode('utf8'))


if __name__ == '__main__':  # for console debugging
    secret = 'TestString From PHP'
    print(secret)
    sv_secret_key = 'sv_secret_key'
    sv_iv = 'sv_iv'

    oSvCipher = SvCipherOpenSsl()
    oSvCipher.set_iv(sv_iv)
    oSvCipher.set_secret_key(sv_secret_key)

    s_enc = oSvCipher.encrypt_str(secret).decode("utf-8")
    print(s_enc)

    print(oSvCipher.decrypt_str(s_enc.encode("utf-8")))
#    oSvCipher = svCipher()
#    sv_secret_key = '34yuhanroxc13234'
#    sv_iv = 'HyuhanroxD567856'
#    oSvCipher.set_iv(sv_iv)
#    oSvCipher.set_secret_key(sv_secret_key)
#
#    enc_text = oSvCipher.encrypt_str('test message')
#    print(enc_text)
#    dec_text = oSvCipher.decrypt_str(enc_text)
#    print(dec_text)
