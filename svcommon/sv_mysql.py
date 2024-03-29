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
import re # https://docs.python.org/3/library/re.html
import ctypes
import platform
import threading

# 3rd party library
import configparser  # https://docs.python.org/3/library/configparser.html
import pymysql  # http://pythonstudy.xyz/python/article/202-MySQL-%EC%BF%BC%EB%A6%AC
from decouple import config  # https://pypi.org/project/python-decouple/

# singleview config
if __name__ == 'svcommon.sv_mysql': # for websocket running
    from svcommon import sv_object
    from django.conf import settings
elif __name__ == 'sv_mysql': # for plugin console debugging
    sys.path.append('../../svdjango')
    import sv_object
    import settings
elif __name__ == '__main__': # for class console debugging
    pass


class SvMySql(sv_object.ISvObject):
    """ mysql operation class based on pymysql library """
    __g_dictRegExQueryFileClassifier = {}  # SQL 유형 구분을 위한 정규식 저장
    __g_dictRegEx = {}  # SQL 분석을 위한 정규식 저장
    __g_dictConfig = {}
    __g_lstHandlingErrCode = [
        1146,  # Table doesn't exist Exception
        1062  # Duplicate entry Exception
    ]
    
    def __init__(self):  #, sCallingFrom=None , dict_brand_info=None):
        # dict_brand_info shoud be streamlined with django_etl in the near futher
        self._g_oLogger = logging.getLogger(__file__)
        self.__g_sAppName = None
        self.__g_oConn = None
        self.__g_oCursor = None
        self.__g_sAbsolutePath = None
        self.__g_dictReservedTag = {}  # sql statement에 이식된 {{tag}} 대치 예약어 사전
        self.__g_dictCompiledSqlStmt = {}
        self.__g_tupErrorCode = None  # store mysql error code if raised
        self.__g_nThreadId = self.__get_thread_id()
        # allocate thread memory to cache compiled stmt
        if self.__g_dictCompiledSqlStmt.get(self.__g_nThreadId, None) is None:
            self.__g_dictCompiledSqlStmt[self.__g_nThreadId] = {}

        self.__g_sAbsolutePath = config('ABSOLUTE_PATH_BOT')

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        self.__disconnect()

    def __del__(self):
        self.__g_sAppName = None
        self.__g_dictConfig = {}
        self.__g_oConn = None
        self.__g_oCursor = None
        self.__g_sAbsolutePath = None
        self.__g_dictReservedTag = {}  # sql statement에 이식된 {{tag}} 대치 예약어 사전
        self.__g_dictCompiledSqlStmt = {}
        self.__g_nThreadId = None
        self.__g_tupErrorCode = None
        self.__disconnect()

    def set_app_name(self, s_app_name=None):
        """
        this method is for django only
        :param s_app_name:
        :return:
        """
        s_sub_path = ''
        s_msg = 'weird app name!  ' + __file__ + ':' + sys._getframe().f_code.co_name
        if s_app_name is None:
            print(s_msg)
            self._g_oLogger.debug(s_msg)
            return
        if s_app_name.find('.') != -1:  # eg., transform.view
            self.__g_sAppName = s_app_name.split('.')[0]
            if self.__g_sAppName == 'svplugins':
                if __name__ == 'svcommon.sv_mysql':  # svextract.plugin_console execution
                    self.__g_sAppName = s_app_name + '.queries.'
                elif __name__ == 'sv_mysql':  # console execution
                    self.__g_sAppName = 'queries.'
                lst_namespace = s_app_name.split('.')
                for sPath in lst_namespace:
                    s_sub_path += '/' + sPath
                del lst_namespace
            else:
                s_sub_path += '/' + self.__g_sAppName
        else:
            print(s_msg)
            self._g_oLogger.debug(s_msg)
        
        self.__g_sAbsolutePath += s_sub_path

    def initialize(self, dict_brand_info=None, s_ext_target_host=None):
        o_config = configparser.ConfigParser()
        if dict_brand_info:  # set only internally separted database is requested
            s_brand_db_config_path = os.path.join(settings.SV_STORAGE_ROOT, str(dict_brand_info['n_acct_id']),
                                                  str(dict_brand_info['n_brand_id']), 'database.config.ini')
            if os.path.isfile(s_brand_db_config_path):
                o_config = configparser.ConfigParser()
                o_config.read(s_brand_db_config_path)
                # self.__g_sDbMode = 'pymysql'
        if s_ext_target_host is None:
            if 'SERVER' in o_config:  # use account specific DB
                self.__g_dictConfig['db_hostname'] = o_config['SERVER']['db_hostname']
                self.__g_dictConfig['db_port'] = int(o_config['SERVER']['db_port'])
                self.__g_dictConfig['db_userid'] = o_config['SERVER']['db_userid']
                self.__g_dictConfig['db_password'] = o_config['SERVER']['db_password']
                self.__g_dictConfig['db_database'] = o_config['SERVER']['db_database']
                self.__g_dictConfig['db_charset'] = o_config['SERVER']['db_charset']
            else:  # use django DB  
                self.__g_dictConfig['db_hostname'] = config('db_hostname')
                self.__g_dictConfig['db_port'] = int(config('db_port'))
                self.__g_dictConfig['db_userid'] = config('db_userid')
                self.__g_dictConfig['db_password'] = config('db_password')
                self.__g_dictConfig['db_database'] = config('db_database')
                self.__g_dictConfig['db_charset'] = config('db_charset')
                #### begin - python dbs.py init to run daemonocle ########
                self.__g_dictConfig['db_table_prefix'] = config('db_table_prefix')  
                #### end - python dbs.py init to run daemonocle ########
        elif s_ext_target_host == 'BI_SERVER':
            self.__g_dictConfig['db_hostname'] = o_config['BI_SERVER']['db_hostname']
            self.__g_dictConfig['db_port'] = int(o_config['BI_SERVER']['db_port'])
            self.__g_dictConfig['db_userid'] = o_config['BI_SERVER']['db_userid']
            self.__g_dictConfig['db_password'] = o_config['BI_SERVER']['db_password']
            self.__g_dictConfig['db_database'] = o_config['BI_SERVER']['db_database']
            self.__g_dictConfig['db_charset'] = o_config['BI_SERVER']['db_charset']
        del o_config
        self.__connect()

        if 'select' not in self.__g_dictRegExQueryFileClassifier:
            self.__g_dictRegExQueryFileClassifier['select'] = re.compile(r"^[g][e][t]\w+")
        if 'update' not in self.__g_dictRegExQueryFileClassifier:
            self.__g_dictRegExQueryFileClassifier['update'] = re.compile(r"^[u][p][d][a][t][e]\w+")
        if 'insert' not in self.__g_dictRegExQueryFileClassifier:
            self.__g_dictRegExQueryFileClassifier['insert'] = re.compile(r"^[i][n][s][e][r][t]\w+")
        if 'delete' not in self.__g_dictRegExQueryFileClassifier:
            self.__g_dictRegExQueryFileClassifier['delete'] = re.compile(r"^[d][e][l][e][t][e]\w+")
        if 'prefix_create_tbl' not in self.__g_dictRegEx:
            self.__g_dictRegEx['prefix_create_tbl'] = re.compile(r"(?<=[cC][rR][eE][aA][tT][eE]\s[tT][aA][bB][lL][eE]\s)\w+")
        if 'hint_select_or_delete' not in self.__g_dictRegEx:
            self.__g_dictRegEx['hint_select_or_delete'] = re.compile(r"(?<=[fF][rR][oO][mM]\s)\w+")
        if 'hint_update' not in self.__g_dictRegEx:
            self.__g_dictRegEx['hint_update'] = re.compile(r"(?<=[uU][pP][dD][aA][tT][eE]\s)\w+")
        if 'hint_insert' not in self.__g_dictRegEx:
            self.__g_dictRegEx['hint_insert'] = re.compile(r"(?<=[iI][nN][tT][oO]\s)\w+")
        if 'retrieve_reserved_tag' not in self.__g_dictRegEx:
            self.__g_dictRegEx['retrieve_reserved_tag'] = re.compile(r"(?<=[{][{]).*?(?=[}][}])")

        if s_ext_target_host is None:  # do not create default table if ext bi db mode
            s_schema_path_abs = os.path.join(self.__g_sAbsolutePath, 'schemas', '')  # to end with '/'
            for _, _, files in os.walk(s_schema_path_abs):
                for filename in files:
                    if not filename.startswith('_'):
                        sTableName = re.sub(".sql", "", filename )
                        self.__create_tbl(sTableName)

    def set_tbl_prefix(self, s_table_prefix):
        if s_table_prefix is not None:
            self.__g_dictConfig['db_table_prefix'] = s_table_prefix+'_'

    def setTablePrefix(self, s_table_prefix):  # will be deprecated 
        self.set_tbl_prefix(s_table_prefix)
                
    def set_reserved_tag_value(self, dict_tag):
        if not dict_tag:
            self._g_oLogger.debug('invalid tag dictionary')
            raise Exception('invalid tag dictionary')
        for tag, value in dict_tag.items():
            self.__g_dictReservedTag[tag] = value

    def truncateTable(self, s_table_name):
        s_real_tbl_name = self.__g_dictConfig['db_table_prefix'] + s_table_name
        s_sql_statement = 'truncate `' + s_real_tbl_name + '`;'
        self.__g_oCursor.execute(s_sql_statement)
    
    def create_table_on_demand(self, s_schema_filename):
        if s_schema_filename.startswith('_'):  # _로 시작하는 스키마 파일은 명시 요청할 때만 생성
            self.__create_tbl(s_schema_filename)

    def executeDynamicQuery(self, s_pysql_id, dict_param):
        """ 
        execute hard-coded sql statement without params
        no cache allowed as this is dynamic query
        dict_param은 msg broker를 통과할 수도 있으므로 문자열 변수만 포함해야 함
        """
        s_query_type, s_sql_compiled = self.__compile_dynamic_sql(s_pysql_id, dict_param)
        if s_query_type == 'unknown':
            return []
        if s_sql_compiled is None:
            return []
        # execute query
        #try:
        # self.__g_oCursor.execute(s_sql_compiled)
        self.__execute_query(s_sql_compiled)
        # except Exception as e:  # eg, Duplicate entry Exception
        #     raise e
        return self.__arrange_query_rst(s_query_type)

    def executeQuery(self, s_sql_filename, *params):  # params is tuple type
        # *params 앞머리에 있는 *는 언팩 연산자.
        # 언팩 연산자는 바인딩 시에 여러 개의 값이 이 이름에 튜플로 바인딩된다는 의미.
        # 튜플 언팩 연산자는 바인딩 구문에서도 활용될 수 있다. 우변의 연속된 값들이 해당 이름의 튜플이 된다는 의미이다.
        # **kwargs에서 **는 이름이 붙은 인자들을 dictionary로 언패킹한다는 의미.
        # no way to pass insert a list-like or comma-delimited one column to *param tuple
        s_query_type, s_sql_compiled = self.__g_dictCompiledSqlStmt[self.__g_nThreadId].get(s_sql_filename, (None,None))
        if s_query_type is None and s_sql_compiled is None:
            s_query_type, s_sql_compiled = self.__compile_static_sql(s_sql_filename)
            if s_query_type == 'unknown':
                return []
            if s_sql_compiled is None:
                return []
            # cache compiled sql statement
            self.__g_dictCompiledSqlStmt[self.__g_nThreadId][s_sql_filename] = [s_query_type, s_sql_compiled]
        #try:
        # self.__g_oCursor.execute(s_sql_compiled, params)
        self.__execute_query(s_sql_compiled, params)  # execute query
        # except Exception as e:  # eg, Duplicate entry Exception
        #     raise e
        return self.__arrange_query_rst(s_query_type)

    def commit(self):
        """
        update stmt on dbs.py requires explicit commit(); dont know why
        """
        if self.__g_oConn:
            self.__g_oConn.commit()

    def disconnect(self):
        self.__disconnect()

    def __execute_query(self, s_sql_compiled, params=None):
        """
        execute query
        :param s_sql_compiled:
        :param params: None if dynamic query
        :return:
        """
        try:
            if params:
                self.__g_oCursor.execute(s_sql_compiled, params)
            else:
                self.__g_oCursor.execute(s_sql_compiled)
        # except utils.OperationalError as e:
        #     if e.args[0] == 2006:  # mysql 2006 error “MySQL server has gone away” exception in Django; this happens only on http connection
        #         # logger.debug('mysql 2006 exception catched... rebuild connection')
        #         self.__connect()
        #         if params:
        #             self.__g_oCursor.execute(s_sql_compiled, params)
        #         else:
        #             self.__g_oCursor.execute(s_sql_compiled)
        # except utils.ProgrammingError as e:
        #     if e.args[0] == 1146:  # Table doesn't exist Exception
        #         pass
        except Exception as e:
            n_pymysql_err_code = e.args[0]
            if n_pymysql_err_code in self.__g_lstHandlingErrCode:
                self.__g_tupErrorCode = e.args
            elif n_pymysql_err_code == 2006:  # mysql 2006 error “MySQL server has gone away” exception in Django; this happens only on http connection
                # logger.debug('mysql 2006 exception catched... rebuild connection')
                self.__connect()
                if params:
                    self.__g_oCursor.execute(s_sql_compiled, params)
                else:
                    self.__g_oCursor.execute(s_sql_compiled)
            else:
                print(e)
                self._g_oLogger.debug(e)
            
    def __get_thread_id(self):
        """
        System dependent, see e.g. /usr/include/x86_64-linux-gnu/asm/unistd_64.h
        Returns OS thread id to identify 
        a cached compiled query for each thread
        """
        s_os_title = platform.system()
        n_thread_id = 0
        if s_os_title == 'Windows':  # windows
            n_thread_id = threading.get_ident()
        elif s_os_title == 'Linux':  
            libc = ctypes.cdll.LoadLibrary('libc.so.6')
            SYS_gettid = 186
            n_thread_id = libc.syscall(SYS_gettid)
        return n_thread_id

    def __import_pysql(self, s_module_name):
        if self.__g_sAppName is None:
            self._g_oLogger.debug('you request dynamic sql but import path is invalid')
            raise Exception('you request dynamic sql but import path is invalid')

        s_module_path = self.__g_sAppName + s_module_name
        try:
            module = __import__(s_module_path, fromlist=[s_module_name])
            return getattr(module, s_module_name)()  # attention to () postfix
        except ModuleNotFoundError as e:
            self._g_oLogger.debug(e)
            raise e

    def __compile_dynamic_sql(self, s_pysql_filename, dict_param):  # add new
        try:
            o_sql = self.__import_pysql(s_pysql_filename)
        except ModuleNotFoundError as e:
            return []
        o_sql.initialize(dict_param)
        s_sql_built = str(o_sql)
        del o_sql
        return self.__compile_sql(s_pysql_filename, s_sql_built)

    def __compile_static_sql(self, s_sql_filename):
        # https://wikidocs.net/26
        try:
            s_sql_path_abs = os.path.join(self.__g_sAbsolutePath, 'queries', s_sql_filename + '.sql')
            f = open(s_sql_path_abs, 'r')
            s_sql_statement = f.read()
            f.close()
        except Exception as e:  # eg., sql file not found
            self._g_oLogger.debug(e)
            raise e  # different with Exception(e)
        return self.__compile_sql(s_sql_filename, s_sql_statement)

    def __compile_sql(self, s_sql_filename, s_sql_statement):  # add new
        s_query_type_by_filename = self.__categorize_qry_by_filename(s_sql_filename)
        s_sql_compiled = re.sub("\n", " ", s_sql_statement)  # make one line
        s_sql_compiled = re.sub("`", "", s_sql_compiled)  # 존재할 수 있는 sql 문자열 wrapper 기호 제거 `
        s_sql_compiled = self.__replace_tag_to_value(s_sql_compiled)
        s_query_type = self.__categorize_qry_by_stmt(s_sql_compiled)
        # validate query type by comparing both of results
        if s_query_type_by_filename != s_query_type:
            return ['unknown', s_sql_compiled]

        if s_query_type == 'select' or s_query_type == 'delete':  # 쿼리 종류 확인 SELECT FROM / DELETE FROM
            result = self.__g_dictRegEx['hint_select_or_delete'].finditer(s_sql_compiled)
        elif s_query_type == 'update':  # 쿼리 종류 확인 UPDATE
            result = self.__g_dictRegEx['hint_update'].finditer(s_sql_compiled)
        elif s_query_type == 'insert':  # 쿼리 종류 확인 INSERT INTO
            result = self.__g_dictRegEx['hint_insert'].finditer(s_sql_compiled)
        else:
            result = None
        if result:
            for r in result:  # add table prefix on table name                
                s_table_name = r.group(0)
                if s_table_name.startswith('_'):  # regarding on demand table name starts with _
                    s_normalized_tbl_name = s_table_name.replace('_', '', 1)
                    s_normalized_tbl_name = self.__g_dictConfig['db_table_prefix'] + s_normalized_tbl_name
                    s_sql_compiled = re.sub(s_table_name, s_normalized_tbl_name, s_sql_compiled)  # 테이블명이 _로 시작하면 on demand table이므로 _ 제거
                else:
                    s_sql_compiled = re.sub(s_table_name, self.__g_dictConfig['db_table_prefix'] + s_table_name, s_sql_compiled)
        del result
        return [s_query_type, s_sql_compiled]
    
    def __replace_tag_to_value(self, s_sql_compiled):
        """ sql statement에 포함된 예약어를 탐색 """ 
        result = self.__g_dictRegEx['retrieve_reserved_tag'].finditer(s_sql_compiled)
        if result is not None:
            for r in result:
                # add table prefix on table name
                s_reserved_tag = r.group(0)
                s_replacing_value = str(self.__g_dictReservedTag[s_reserved_tag])
                s_sql_compiled = re.sub('{{' + s_reserved_tag + '}}', s_replacing_value, s_sql_compiled)
        del result
        return s_sql_compiled

    def __arrange_query_rst(self, s_query_type):
        """ param: s_sql_filename is for query debug """
        if  self.__g_tupErrorCode is not None:
            return [{'err_code': self.__g_tupErrorCode[0], 'err_msg': self.__g_tupErrorCode[1]}]

        if s_query_type == 'insert':
            # https://stackoverflow.com/questions/2548493/how-do-i-get-the-id-after-insert-into-mysql-database-with-python
            lst_rows = [{'id': self.__g_oCursor.lastrowid}]
        elif s_query_type == 'delete':
            # self.__g_oConn.commit()
            lst_rows = [{'rowcount': self.__g_oCursor.rowcount}]
        else:
            lst_rows = self.__fetch()
            if len(lst_rows) == 0:
                lst_rows = []
        return lst_rows

    def __categorize_qry_by_filename(self, s_sql_filename):
        for key, o_regex in self.__g_dictRegExQueryFileClassifier.items():
            m = o_regex.search(s_sql_filename)
            if m is not None:
                return key
        return 'unknown'

    def __categorize_qry_by_stmt(self, s_sql_built):
        s_query_type = 'unknown'
        lst_first_chunk = s_sql_built.split(' ')
        try:
            s_query_type = lst_first_chunk[0].lower()
        except Exception as e:
            self._g_oLogger.debug(e)
            pass
        lst_first_chunk.clear()
        return s_query_type
        
    def __fetch(self):
        # Fetch data
        return self.__g_oCursor.fetchall()

    def __connect(self):
        # MySQL Connection 연결
        self.__g_oConn = pymysql.connect(host=self.__g_dictConfig['db_hostname'], port=self.__g_dictConfig['db_port'], user=self.__g_dictConfig['db_userid'], 
            password=self.__g_dictConfig['db_password'], db=self.__g_dictConfig['db_database'], charset=self.__g_dictConfig['db_charset'])
        # Connection 으로부터 Cursor 생성
        self.__g_oCursor = self.__g_oConn.cursor(pymysql.cursors.DictCursor) # set Dictionary cursor, Array based cursor if None

    def __disconnect(self):
        # unset, if thread memory for cached compiled stmt exists
        if self.__g_dictCompiledSqlStmt.get(self.__g_nThreadId, None):
            del self.__g_dictCompiledSqlStmt[self.__g_nThreadId]
        # Connection close
        if self.__g_oConn is not None and self.__g_oConn.open:
            try:
               self.__g_oConn.cursor()
            except NameError: #	disconnected, whatever error raised, no difference
                pass
            else: # connected
                self.__g_oConn.close()

    def __create_tbl(self, s_tbl_name):
        # https://wikidocs.net/26
        s_schema_path_abs = os.path.join(self.__g_sAbsolutePath, 'schemas', s_tbl_name + '.sql')
        f = open(s_schema_path_abs,'r')
        s_sql_stmt = f.read()
        f.close()
        s_sql_stmt = re.sub("\n", " ", s_sql_stmt)  # make one line
        s_sql_stmt = re.sub("`", "", s_sql_stmt ) # 존재할 수 있는 sql 문자열 wrapper 기호 제거 `
        if s_tbl_name.startswith('_'):  # regarding on demand table name starts with _
            s_normalized_tbl_name = s_tbl_name.replace('_', '', 1)
            s_sql_stmt = re.sub(s_tbl_name, s_normalized_tbl_name, s_sql_stmt )  # 테이블명이 _로 시작하면 on demand table이므로 _ 제거
        # m = oRegEx.search(s_sql_stmt) # match()와 search() 차이점 refer to 빠르게활용하는파이썬3.6프로그램 p241
        m = self.__g_dictRegEx['prefix_create_tbl'].search(s_sql_stmt)
        if m: # if table name is existing in the sql statement
            s_tbl_search_qry = "show tables like '"+self.__g_dictConfig['db_table_prefix']+m.group(0)+"'" # add table prefix on table name
            self.__g_oCursor.execute(s_tbl_search_qry)
            rows = self.__g_oCursor.fetchall()
            if not self.__g_oCursor.rowcount: # table creation
                if __name__ == '__main__':
                    self._printDebug("table creation")
                s_sql_stmt = re.sub(m.group(0), self.__g_dictConfig['db_table_prefix']+m.group(0), s_sql_stmt ) # add table prefix on table name
                self.__g_oCursor.execute(s_sql_stmt)
            else:
                if __name__ == '__main__':
                    self._printDebug("already existing table")
    

# if __name__ == '__main__': # for console debugging
#     with SvMySql('job_plugins.nvadperformance_period') as oSvMysql:
#         lstRows = oSvMysql.executeQuery('getJobList', 'Y')
#         #lstRows = oSvMysql.executeQuery('updateJobDetail', '20181112255522', '1' )
#         print(lstRows)
