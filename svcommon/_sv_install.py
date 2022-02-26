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
from pathlib import Path
import configparser

# 3rd party library

# singleview library
if __name__ == '__main__': # for class console debugging
    import sys
    sys.path.append('../classes')
    import sv_cipher
elif __name__ == 'sv_http': # for plugin console debugging
    import sys
    sys.path.append('../../classes')
    import sv_cipher
elif __name__ == 'classes.sv_http' : # for platform running
    from classes import sv_cipher

class SvInstall():
    """ install class """
    __g_sBaseAbsPath = None
    __g_sMiscAbsPath = None
    __g_sConfAbsPath = None

    def __init__(self):
        pass
    
    def start_up_job_plugin(self, dict_config_info):
        """run plugin for the first time"""
        print('start_up_job_plugin')

        self.__g_sBaseAbsPath = dict_config_info['s_abs_path_bot']
        self.__g_sMiscAbsPath = os.path.join(self.__g_sBaseAbsPath, 'misc')
        self.__g_sConfAbsPath = os.path.join(self.__g_sBaseAbsPath, 'conf')

        lst_acct_info = dict_config_info['s_acct_info'].split('/')
        s_account_directory = os.path.join(self.__g_sBaseAbsPath, 'files', lst_acct_info[0], lst_acct_info[1])
        del lst_acct_info

        # mkdir account directory
        Path(s_account_directory).mkdir(parents=True, exist_ok=True)
        s_api_info_file_abs_path = os.path.join(self.__g_sMiscAbsPath, 'api_info.template')

        o_config = configparser.ConfigParser()
        o_config.read(s_api_info_file_abs_path)
        # print(type(o_config))  # <class 'configparser.ConfigParser'>
        o_config['naver_searchad']['manager_login_id'] = 'final'
        del o_config['google_ads']['123-456-7890']
        
        s_api_info_ini_abs_path = os.path.join(s_account_directory, 'api_info.ini')
        with open(s_api_info_ini_abs_path, 'w') as configfile:    # save
            o_config.write(configfile)
        del o_config

    def start_up_dbs(self):
        """run dbs for the first time"""
        self.__g_sBaseAbsPath = Path(__file__).parent.parent.resolve()
        self.__g_sMiscAbsPath = os.path.join(self.__g_sBaseAbsPath, 'misc')
        self.__g_sConfAbsPath = os.path.join(self.__g_sBaseAbsPath, 'conf')
        # mkdir conf/
        Path(self.__g_sConfAbsPath).mkdir(parents=True, exist_ok=True)
        
        # write conf/basic_config.py
        s_debug_input = input("is debug mode(Yes/no): ")
        if s_debug_input.lower() == 'yes':
            s_debug_mode = 'devel'
        else:
            s_debug_mode = 'release'
        del s_debug_input

        s_config_template = self.__get_config_template('basic_config')
        s_filled_template = s_config_template % (self.__g_sBaseAbsPath, s_debug_mode)
        s_conf_file_abs_path = os.path.join(self.__g_sConfAbsPath, 'basic_config.py')
        with open(s_conf_file_abs_path, "w") as f:
            f.write(s_filled_template)
        del s_config_template, s_filled_template

        # write conf/mysql_config.ini
        s_db_userid = input("mysql on localhost user id: ")
        s_db_password = input("mysql on localhost user password: ")
        s_db_database = input("mysql on localhost database name: ")
        s_db_table_prefix = input("table prefix to identify (default is sv): ")
        if len(s_db_table_prefix) == 0:
            s_db_table_prefix = 'sv'
        
        s_config_template = self.__get_config_template('mysql_config')
        s_filled_template = s_config_template % (s_db_userid, s_db_password, s_db_database, s_db_table_prefix)
        s_conf_file_abs_path = os.path.join(self.__g_sConfAbsPath, 'mysql_config.ini')
        with open(s_conf_file_abs_path, "w") as f:
            f.write(s_filled_template)
        del s_db_userid, s_db_password, s_db_database, s_db_table_prefix
        del s_config_template, s_filled_template

        try:  # check representative dependency
            import daemonocle
        except ModuleNotFoundError: 
            print('you should run pip install -r requirements.txt')

    def __get_config_template(self, s_template_name):
        s_template_filename = s_template_name + '.template'
        s_conf_template_file_abs_path = os.path.join(self.__g_sMiscAbsPath, s_template_filename)
        # print(s_conf_template_file_abs_path)
        with open(s_conf_template_file_abs_path, "r") as f:
            lst_lines = f.readlines()
            
        s_basic_config_content = ''.join(lst_lines)
        del lst_lines
        return s_basic_config_content
