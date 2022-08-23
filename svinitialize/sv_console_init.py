import os
import sys
import random


class svInitialize():
    __g_sEnvFileName = '.env'
    __g_bInitMode = False
    __g_sEnvPath = None
    __g_sRootPath = None
    """
    Encapsulate the logic of the django-admin and manage.py utilities.
    """
    def __init__(self, argv=None):
        self.argv = argv or sys.argv[:]
        
        self.__g_sRootPath = os.getcwd()
        self.__g_sEnvPath = os.path.join(self.__g_sRootPath, self.__g_sEnvFileName)

        if not os.path.isfile(self.__g_sEnvPath):
            self.__g_bInitMode = True

    def execute(self):
        """
        Given the command-line arguments, figure out which subcommand is being
        run, create a parser appropriate to that command, and run it.
        """
        try:
            subcommand = self.argv[1]
        except IndexError:
            return 'pass'

        if not self.__g_bInitMode and subcommand != 'svinitialize':
            return 'pass'

        if self.__check_installed():
            s_denial_msg = """A service has been already installed\nIf you want to reinstall,\nplease remove {} and retry\n""".format(self.__g_sEnvPath)
            sys.stdout.write(s_denial_msg)
            return 'stop'

        sys.stdout.write('console initialize\n')
        # import module test
        try:
            import django
            import decouple
            import pymysql
            import widget_tweaks
            import channels
        except ImportError:
            sys.stdout.write('run "pip install -r requirements.txt"\n')
            return 'stop'

        # enquire configuration info
        s_allowed_host_csv = str(input("csv formatted allowed domain name for django running\n('localhost' and server IP are not necessary): "))

        try:
            n_mysql_db_port = int(input("mysql_db_port(3306 if blank): "))
        except ValueError:
            n_mysql_db_port = 3306

        s_db_hostname = str(input("mysql_db_host_name(127.0.0.1 if blank): "))
        if len(s_db_hostname) == 0:
            s_db_hostname = '127.0.0.1'
        s_db_charset = str(input("mysql_db_charset(utf8 if blank): "))
        if len(s_db_charset) == 0:
            s_db_charset = 'utf8'
        
        lst_mysql_config = ['mysql_user_id: ', 'mysql_user_password: ', 'mysql_database_name: ', 'table_prefix(no underbar): ']
        lst_param_val = []
        for s_input_msg in lst_mysql_config:
            s_tmp_param = str(input(s_input_msg))
            while True:
                if len(s_tmp_param) == 0:
                    s_tmp_param = str(input(s_input_msg))
                else:
                    lst_param_val.append(s_tmp_param)
                    break
        lst_param_val[3] = lst_param_val[3].replace('_', '')  # _ is disallowed for table_prefix

        # mysql test connection
        try:
            pymysql.connect(host=s_db_hostname, user=lst_param_val[0], password=lst_param_val[1], db=lst_param_val[2], charset=s_db_charset)
        except pymysql.err.OperationalError as e:
            sys.stdout.write(str(e)+'\n')
            return 'stop'

        # write .env file.
        s_template = self.__read_template_file(self.__g_sEnvFileName)
        with open(self.__g_sEnvPath, 'w', encoding='utf-8') as out:
           out.write(s_template.format(self.__g_sRootPath, 'True', self.__get_secret_key(), s_allowed_host_csv,
                    str(n_mysql_db_port), s_db_hostname, lst_param_val[0], lst_param_val[1], lst_param_val[2], s_db_charset, lst_param_val[3]))

        sys.stdout.write('console initialization has been completed\nrun python manage.py runserver 0.0.0.0:8000\n')
        return 'stop'

    def __check_installed(self):
        try:
            f = open(self.__g_sEnvPath, 'r')
            f.close()
            return True
        except FileNotFoundError:
            return False

    def __get_secret_key(self):
        n_namespace_len = 50  # refer to column max_length
        s_allowed_chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefghjklmnopqrstuvwxyz_!@#$%^&*()-+=?/.,<>:;'
        s_u_namespace = ''.join(random.sample(s_allowed_chars, len(s_allowed_chars)))
        return s_u_namespace[:n_namespace_len]

    def __read_template_file(self, s_template_name):
        s_conf_templ = os.path.join(self.__g_sRootPath, 'svinitialize', 'templates', s_template_name + '.template')
        s_template = ''
        with open(s_conf_templ, 'r', encoding='utf-8') as f:
            while True:
                s_line = f.readline()
                if not s_line: 
                    break
                s_template = s_template + s_line
        return s_template


#def init_from_command_line(argv=None):
#    """Run a ManagementUtility."""
#    utility = ManagementUtility(argv)
#    s_msg = utility.execute()
#    if s_msg != 'pass':
#        sys.exit(0)
