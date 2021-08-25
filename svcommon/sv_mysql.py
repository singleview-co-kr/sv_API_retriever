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
import re # https://docs.python.org/3/library/re.html

# 3rd party library
import pymysql  # http://pythonstudy.xyz/python/article/202-MySQL-%EC%BF%BC%EB%A6%AC

# singleview config
if __name__ == 'svcommon.sv_mysql': # for platform running
    from conf import basic_config
    from svcommon import sv_object
elif __name__ == 'sv_mysql': # for plugin console debugging
    sys.path.append('../../conf')
    import basic_config
    import sv_object
elif __name__ == '__main__': # for class console debugging
    sys.path.append('../conf')
    import basic_config

class SvMySql(sv_object.ISvObject):
    """ mysql operation class based on pymysql library """
    __g_sAppName = None
    __g_oConfig = None
    __g_oConn = None
    __g_oCursor = None
    __g_sAbsolutePath = None
    __g_dictRegExQueryFileClassifier = {}  # SQL 유형 구분을 위한 정규식 저장
    __g_dictRegEx = {}  # SQL 분석을 위한 정규식 저장
    __g_dictReservedTag = {}  # sql statement에 이식된 {{tag}} 대치 예약어 사전
    __g_dictCompiledSqlStmt = {}

    def __init__(self, sCallingFrom=None):
        self.__g_oConfig = configparser.ConfigParser()
        self._g_oLogger = logging.getLogger(__file__)
        
        self.__g_sAbsolutePath = basic_config.ABSOLUTE_PATH_BOT
        if sCallingFrom is not None:
            if __name__ == 'svcommon.sv_mysql':  # svextract.plugin_console execution
                self.__g_sAppName = sCallingFrom + '.queries.'
            elif __name__ == 'sv_mysql':  # console execution
                self.__g_sAppName = 'queries.'
            lstNameSpace = sCallingFrom.split('.')
            sSubPath = ''
            for sPath in lstNameSpace:
                sSubPath += '/' + sPath
            self.__g_sAbsolutePath += sSubPath
        
        sMysqlConfigFile = os.path.join(basic_config.ABSOLUTE_PATH_BOT, 'conf', 'mysql_config.ini')
        try:
            with open(sMysqlConfigFile) as f:
                self.__g_oConfig.read_file(f)
        except IOError:
            self._printDebug('config.ini not exist')
            raise IOError('failed to initialize SvMySql')

        self.__g_oConfig.read(sMysqlConfigFile)
        self.__connect()

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        self.__disconnect()

    def __del__(self):
        self.__disconnect()
        
    def initialize(self):  # will be moved to somewhere in dbs.py
        self.__g_dictRegExQueryFileClassifier['select'] = re.compile(r"^[g][e][t]\w+")
        self.__g_dictRegExQueryFileClassifier['update'] = re.compile(r"^[u][p][d][a][t][e]\w+")
        self.__g_dictRegExQueryFileClassifier['insert'] = re.compile(r"^[i][n][s][e][r][t]\w+")
        self.__g_dictRegExQueryFileClassifier['delete'] = re.compile(r"^[d][e][l][e][t][e]\w+")

        self.__g_dictRegEx['prefix_create_tbl'] = re.compile(r"(?<=[cC][rR][eE][aA][tT][eE]\s[tT][aA][bB][lL][eE]\s)\w+")
        self.__g_dictRegEx['hint_select_or_delete'] = re.compile(r"(?<=[fF][rR][oO][mM]\s)\w+")
        self.__g_dictRegEx['hint_update'] = re.compile(r"(?<=[uU][pP][dD][aA][tT][eE]\s)\w+")
        self.__g_dictRegEx['hint_insert'] = re.compile(r"(?<=[iI][nN][tT][oO]\s)\w+")
        self.__g_dictRegEx['retrieve_reserved_tag'] = re.compile(r"(?<=[{][{]).*?(?=[}][}])")

        #self.__connect()
        # oRegEx = re.compile(r"(?<=[cC][rR][eE][aA][tT][eE]\s[tT][aA][bB][lL][eE]\s)\w+")
        # for root, dirs, files in os.walk(self.__g_sAbsolutePath+'/schemas/'):
        s_schema_path_abs = os.path.join(self.__g_sAbsolutePath, 'schemas', '')  # to end with '/'
        for root, dirs, files in os.walk(s_schema_path_abs):
            for filename in files:
                if not filename.startswith('_'):
                    sTableName = re.sub(".sql", "", filename )
                    # self.__createTable(sTableName, oRegEx)
                    self.__createTable(sTableName)
        #self.__disconnect()

    def setTablePrefix(self, sTablePrefix):
        if( sTablePrefix is not None ):
            self.__g_oConfig['SERVER']['db_table_prefix'] = sTablePrefix+'_'
            # self.__g_oConfig['SERVER']['db_table_prefix'] = sTablePrefix
            # if sTablePrefix.find('_') == -1:
	        #    self.__g_oConfig['SERVER']['db_table_prefix'] = self.__g_oConfig['SERVER']['db_table_prefix'] + '_'
    
    def set_reserved_tag_value(self, dict_tag):
        if not dict_tag:
            raise Exception('invalid tag dictionary')
        for tag, value in dict_tag.items():
            self.__g_dictReservedTag[tag] = value

    def truncateTable(self, sTableName):
        sRealTableName = self.__g_oConfig['SERVER']['db_table_prefix'] + sTableName
        sSqlStatement = 'truncate `' + sRealTableName + '`;'
        self.__g_oCursor.execute(sSqlStatement)
    
    def createTableOnDemand(self, s_prefix, s_schema_filename):
        if s_schema_filename.startswith('_'):  # _로 시작하는 스키마 파일은 필요할 때만 생성
            # tbl prefix를 임시 변경
            # s_tbl_prefix_origin = self.__g_oConfig['SERVER']['db_table_prefix']
            # self.__g_oConfig['SERVER']['db_table_prefix'] = s_prefix  # create 문의 table 이름이 _로 시작함.
            # o_reg_ex = re.compile(r"(?<=[cC][rR][eE][aA][tT][eE]\s[tT][aA][bB][lL][eE]\s)\w+")
            # self.__createTable(s_schema_filename, o_reg_ex)
            self.__createTable(s_schema_filename)
            # tbl prefix를 복원
            # self.__g_oConfig['SERVER']['db_table_prefix'] = s_tbl_prefix_origin

    def executeDynamicQuery(self, s_pysql_id, dict_param):
        # execute hard-coded sql statement without params
        # no cache allowed as this is dynamic query
        # dict_param은 msg broker를 통과할 수도 있으므로 문자열 변수만 포함해야 함
        s_query_type, s_sql_compiled = self.__compileDynamicSql(s_pysql_id, dict_param)
        if s_query_type == 'unknown':
            return []
        if s_sql_compiled is None:
            return []
        # execute query
        try:
            self.__g_oCursor.execute(s_sql_compiled)
        except Exception as e:  # eg, Duplicate entry Exception
            raise e
        # return dataset
        return self.__arrange_query_rst(s_pysql_id, s_query_type)

    def executeQuery(self, s_sql_filename, *params):  # params is tuple type
        # *params 앞머리에 있는 *는 언팩 연산자.
        # 언팩 연산자는 바인딩 시에 여러 개의 값이 이 이름에 튜플로 바인딩된다는 의미.
        # 튜플 언팩 연산자는 바인딩 구문에서도 활용될 수 있다. 우변의 연속된 값들이 해당 이름의 튜플이 된다는 의미이다.
        # **kwargs에서 **는 이름이 붙은 인자들을 dictionary로 언패킹한다는 의미.
        # no way to pass insert a list-like or comma-delimited one column to *param tuple
        # cache a prepared query statement
        try:
            s_query_type, s_sql_compiled = self.__g_dictCompiledSqlStmt[s_sql_filename]
        except KeyError:
            s_query_type, s_sql_compiled = self.__compileStaticSql(s_sql_filename)
            if s_query_type == 'unknown':
                return []
            if s_sql_compiled is None:
                return []
            # cache compiled sql statement
            self.__g_dictCompiledSqlStmt[s_sql_filename] = [s_query_type, s_sql_compiled]
        # execute query
        try:
            self.__g_oCursor.execute(s_sql_compiled, params)
        except Exception as e:  # eg, Duplicate entry Exception
            raise e
        # return dataset
        return self.__arrange_query_rst(s_sql_filename, s_query_type)

    def __import_pysql(self, s_module_name):
        if self.__g_sAppName is None:
            raise Exception('you request dynamic sql but import path is invalid')

        s_module_path = self.__g_sAppName + s_module_name
        try:
            module = __import__(s_module_path, fromlist=[s_module_name])
            return getattr(module, s_module_name)()  # attention to () postfix
        except ModuleNotFoundError as e:
            raise e

    def __compileDynamicSql(self, s_pysql_filename, dict_param):  # add new
        # s_pysql_path_full = self.__g_sAppName + '.queries.' + s_pysql_filename
        try:
            # o_sql = self.__import_pysql(s_pysql_path_full, s_pysql_filename)
            o_sql = self.__import_pysql(s_pysql_filename)
        except ModuleNotFoundError as e:
            return []
        o_sql.initialize(dict_param)
        s_sql_built = str(o_sql)
        del o_sql
        return self.__compileSql(s_pysql_filename, s_sql_built)
        # return [s_query_type, s_sql_compiled]

    def __compileStaticSql(self, s_sql_filename):
        # https://wikidocs.net/26
        try:
            s_sql_path_abs = os.path.join(self.__g_sAbsolutePath, 'queries', s_sql_filename + '.sql')
            f = open(s_sql_path_abs, 'r')
            s_sql_statement = f.read()
            f.close()
        except Exception as e:  # eg., sql file not found
            raise e  # different with Exception(e)
        # print('__compileStaticSql() called for ->' + s_sql_filename)
        return self.__compileSql(s_sql_filename, s_sql_statement)

    def __compileSql(self, s_sql_filename, s_sql_statement):  # add new
        s_query_type_by_filename = self.__categorizeQueryBySqlFilename(s_sql_filename)
        s_sql_compiled = re.sub("\n", " ", s_sql_statement)  # make one line
        s_sql_compiled = re.sub("`", "", s_sql_compiled)  # 존재할 수 있는 sql 문자열 wrapper 기호 제거 `
        s_sql_compiled = self.__replace_tag_to_value(s_sql_compiled)
        s_query_type = self.__categorizeQueryBySqlStatement(s_sql_compiled)

        # validate query type by comparing both of results
        if s_query_type_by_filename != s_query_type:
            return ['unknown', s_sql_compiled]

        # add prefix to tbl name
        # if s_query_type == 'select' or s_query_type == 'delete':  # 쿼리 종류 확인 SELECT FROM / DELETE FROM
        #    o_reg_ex = re.compile(r"(?<=[fF][rR][oO][mM]\s)\w+")
        # elif s_query_type == 'update':  # 쿼리 종류 확인 UPDATE
        #    o_reg_ex = re.compile(r"(?<=[uU][pP][dD][aA][tT][eE]\s)\w+")
        # elif s_query_type == 'insert':  # 쿼리 종류 확인 INSERT INTO
        #    o_reg_ex = re.compile(r"(?<=[iI][nN][tT][oO]\s)\w+")
        # result = o_reg_ex.finditer(s_sql_compiled)

        if s_query_type == 'select' or s_query_type == 'delete':  # 쿼리 종류 확인 SELECT FROM / DELETE FROM
            result = self.__g_dictRegEx['hint_select_or_delete'].finditer(s_sql_compiled)
        elif s_query_type == 'update':  # 쿼리 종류 확인 UPDATE
            result = self.__g_dictRegEx['hint_update'].finditer(s_sql_compiled)
        elif s_query_type == 'insert':  # 쿼리 종류 확인 INSERT INTO
            result = self.__g_dictRegEx['hint_insert'].finditer(s_sql_compiled)
        else:
            result = None
        if result is not None:
            for r in result:
                # add table prefix on table name
                s_table_name = r.group(0)
                s_sql_compiled = re.sub(s_table_name, self.__g_oConfig['SERVER']['db_table_prefix'] + s_table_name, s_sql_compiled)
        del result
        # m = o_reg_ex.search(s_sql_compiled)  # match()와 search() 차이점 refer to 빠르게활용하는파이썬3.6프로그램 p241
        # if m is not None:
        #    # add table prefix on table name
        #    s_table_name = m.group(0)
        #    s_sql_compiled = re.sub(s_table_name, self.__g_oConfig['SERVER']['db_table_prefix'] + s_table_name, s_sql_compiled)

        return [s_query_type, s_sql_compiled]
    
    def __replace_tag_to_value(self, s_sql_compiled):
        # sql statement에 포함된 예약어를 탐색
        # o_replacement_regex = re.compile(r"(?<=[{][{]).*?(?=[}][}])")
        # result = o_replacement_regex.finditer(s_sql_compiled)
        result = self.__g_dictRegEx['retrieve_reserved_tag'].finditer(s_sql_compiled)
        if result is not None:
            for r in result:
                # add table prefix on table name
                s_reserved_tag = r.group(0)
                s_replacing_value = str(self.__g_dictReservedTag[s_reserved_tag])
                s_sql_compiled = re.sub('{{' + s_reserved_tag + '}}', s_replacing_value, s_sql_compiled)
        # del o_replacement_regex
        del result
        return s_sql_compiled

    def __arrange_query_rst(self, s_sql_filename, s_query_type):
        if s_query_type == 'insert':
            # https://stackoverflow.com/questions/2548493/how-do-i-get-the-id-after-insert-into-mysql-database-with-python
            lst_rows = [{'id': self.__g_oCursor.lastrowid}]
        elif s_query_type == 'delete':
            # self.__g_oConn.commit()
            lst_rows = [{'rowcount': self.__g_oCursor.rowcount}]
        else:
            lst_rows = self.__fetch()

        return lst_rows  # return dataset

    def __categorizeQueryBySqlFilename(self, s_sql_filename):
        # dictQueryType = {'select': r"^[g][e][t]\w+", 'update': r"^[u][p][d][a][t][e]\w+",
        #                 'insert': r"^[i][n][s][e][r][t]\w+", 'delete': r"^[d][e][l][e][t][e]\w+"}
        # for key, sregex in dictQueryType.items():
        #    oRegEx = re.compile(sregex)
        #    m = oRegEx.search(s_sql_filename)
        #    if m is not None:
        #        return key
        for key, o_regex in self.__g_dictRegExQueryFileClassifier.items():
            # oRegEx = re.compile(sregex)
            m = o_regex.search(s_sql_filename)
            if m is not None:
                return key
        return 'unknown'

    def __categorizeQueryBySqlStatement(self, s_sql_built):
        s_query_type = 'unknown'
        lst_first_chunk = s_sql_built.split(' ')
        try:
            s_query_type = lst_first_chunk[0].lower()
        except Exception:
            pass
        lst_first_chunk.clear()
        return s_query_type
        
    def __fetch(self):
        # Fetch data
        return self.__g_oCursor.fetchall()

    def __connect(self):
        # MySQL Connection 연결
        self.__g_oConn = pymysql.connect(host=self.__g_oConfig['SERVER']['db_hostname'], user=self.__g_oConfig['SERVER']['db_userid'], 
            password = self.__g_oConfig['SERVER']['db_password'], db=self.__g_oConfig['SERVER']['db_database'], charset=self.__g_oConfig['SERVER']['db_charset'])
        
        # Connection 으로부터 Cursor 생성
        self.__g_oCursor = self.__g_oConn.cursor(pymysql.cursors.DictCursor) # set Dictionary cursor, Array based cursor if None

    def __disconnect(self):
        # Connection 닫기
        if self.__g_oConn is not None and self.__g_oConn.open:
            try:
               c = self.__g_oConn.cursor()
            except NameError: #	disconnected, whatever error raised, no difference
                pass
            else: # connected
                self.__g_oConn.close()

    # def __createTable(self,sTableName,oRegEx):
    def __createTable(self,sTableName):
        # https://wikidocs.net/26
        # f = open(self.__g_sAbsolutePath+'/schemas/'+sTableName+'.sql','r')
        s_schema_path_abs = os.path.join(self.__g_sAbsolutePath, 'schemas', sTableName + '.sql')
        f = open(s_schema_path_abs,'r')
        sSqlStatement = f.read()
        f.close()

        sSqlStatement = re.sub("\n", " ", sSqlStatement)  # make one line
        sSqlStatement = re.sub("`", "", sSqlStatement ) # 존재할 수 있는 sql 문자열 wrapper 기호 제거 `
        # m = oRegEx.search(sSqlStatement) # match()와 search() 차이점 refer to 빠르게활용하는파이썬3.6프로그램 p241
        m = self.__g_dictRegEx['prefix_create_tbl'].search(sSqlStatement)
        if m: # if table name is existing in the sql statement
            sTableSearchSql = "show tables like '"+self.__g_oConfig['SERVER']['db_table_prefix']+m.group(0)+"'" # add table prefix on table name
            self.__g_oCursor.execute(sTableSearchSql)
            rows = self.__g_oCursor.fetchall()
            if not self.__g_oCursor.rowcount: # table creation
                if __name__ == '__main__':
                    self._printDebug("table creation")
                sSqlStatement = re.sub(m.group(0), self.__g_oConfig['SERVER']['db_table_prefix']+m.group(0), sSqlStatement ) # add table prefix on table name
                self.__g_oCursor.execute(sSqlStatement)
            else:
                if __name__ == '__main__':
                    self._printDebug("already existing table")
    
    # def set_app_name(self, s_app_name):
    #     ##### will be deprecated ############
    #     # for executeDynamicQuery() method only
    #     # default is 'queries', if this method does not run 
    #     if s_app_name == '__main__':  # eg., transform.view
    #         self.__g_sAppName = 'queries.'
    #     elif s_app_name == 'svplugins.sv_collect_doc_com.task':  # eg., svextract.plugin_console
    #         self.__g_sAppName = 'svplugins.sv_collect_doc_com.queries.'
    #     else:
    #         print('weird app name!  ' + __file__ + ':' + sys._getframe().f_code.co_name)


if __name__ == '__main__': # for console debugging
    with SvMySql('job_plugins.nvadperformance_period') as oSvMysql:
        lstRows = oSvMysql.executeQuery('getJobList', 'Y')
        #lstRows = oSvMysql.executeQuery('updateJobDetail', '20181112255522', '1' )
        print(lstRows)
