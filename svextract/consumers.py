# standard library
import os
import sys
import importlib
import json
import threading
from svauth.models import User
from channels.generic.websocket import WebsocketConsumer

# singleview config
# from conf import basic_config
from decouple import config

# install WebSocket
# https://ssungkang.tistory.com/entry/Django-Channels-%EB%B9%84%EB%8F%99%EA%B8%B0%EC%A0%81-%EC%B1%84%ED%8C%85-%EA%B5%AC%ED%98%84%ED%95%98%EA%B8%B0-WebSocket-1
# # pip install -U channels


class ThreadWithTrace(threading.Thread):
    # https://www.geeksforgeeks.org/python-different-ways-to-kill-a-thread/

    def __init__(self, *args, **keywords):
        threading.Thread.__init__(self, *args, **keywords)
        self.killed = False
    
    def start(self):
        self.__run_backup = self.run
        self.run = self.__run     
        threading.Thread.start(self)
    
    def globaltrace(self, frame, event, arg):
        if event == 'call':
            return self.localtrace
        else:
            return None
 
    def localtrace(self, frame, event, arg):
        if self.killed:
            if event == 'line':
                 raise SystemExit()
        return self.localtrace
 
    def force_to_kill(self):
        """ only for forcing to stop while runing 
            python thread kill by itself otherwise
        """
        self.killed = True
    
    def __run(self):
        sys.settrace(self.globaltrace)
        self.__run_backup()
        self.run = self.__run_backup


class PluginConsole(WebsocketConsumer):
    __g_sPluginPathAbs = None
    __g_sAbsPathBot = None
    __g_dictPluginThread = {}  # should improve to a multiuser threading use case
    __g_sSvNull = '#%'
    
    # websocket 연결 시 실행
    def connect(self):
        # print('ws connect')
        # https://stackoverflow.com/questions/65482182/problem-changing-from-websocketconsumer-to-asyncwebsocketconsumer
        self.user = self.scope['user']
        self.__g_sAbsPathBot = config('ABSOLUTE_PATH_BOT')
        self.accept()

    # websocket 연결 종료 시 실행
    def disconnect(self, close_code):
        # print('ws disconnect')
        self.__halt_all_thread()
    
    def print_msg_socket(self, s_msg):
        # Send message to WebSocket
        self.send(text_data=json.dumps({
            'message': s_msg
        }))

    def receive(self, text_data):
        """ run when text_data has been received from a client """
        # https://channels.readthedocs.io/en/latest/topics/routing.html
        # s_b1rand_name = 'test_brand'  # self.scope["url_route"]["kwargs"]["brand_name"]
        s_sv_acct_id = self.scope["url_route"]["kwargs"]["sv_acct_id"]
        s_sv_brand_id = self.scope["url_route"]["kwargs"]["sv_brand_id"]
        s_plugin_unique_id = s_sv_acct_id + '_' + s_sv_brand_id

        try:
            User.objects.get(id=self.user.pk)
        except User.DoesNotExist:
            self.print_msg_socket('invalid user')

        text_data_json = json.loads(text_data)
        s_command = text_data_json['command'].strip()

        # loop back the msg from the client
        self.print_msg_socket('$ ' + s_command)
        if len(s_command) == 0:
            self.print_msg_socket('invalid command')
            return
        lst_command = s_command.split(' ')
        s_plugin_name = lst_command[0].strip()
        if s_plugin_name == 'stop':
            s_interrupt_plugin_name = lst_command[1].strip()
            self.print_msg_socket(s_interrupt_plugin_name + ' is trying to be interrupted!')
            try:
                self.__g_dictPluginThread[s_plugin_unique_id][s_interrupt_plugin_name].force_to_kill()
                # join() means to wait for thread completion, not stopping thread
                self.__g_dictPluginThread[s_plugin_unique_id][s_interrupt_plugin_name].join()
                self.print_msg_socket(s_interrupt_plugin_name + ' has been interrupted!')
                del self.__g_dictPluginThread[s_plugin_unique_id][s_interrupt_plugin_name]
            except KeyError:
                self.print_msg_socket(s_interrupt_plugin_name + ' is unable to be interrupted - not running')
            return
        
        self.__g_sPluginPathAbs = os.path.join(self.__g_sAbsPathBot, 'svplugins', s_plugin_name)
        if not self.__validate_plugin():
            self.print_msg_socket('invalid plugin')
            return

        lst_command.append('config_loc='+ s_sv_acct_id + '/' + s_sv_brand_id)
        # prevent duplicated plugin request
        if s_plugin_unique_id in list(self.__g_dictPluginThread.keys()):  
            if s_plugin_name in list(self.__g_dictPluginThread[s_plugin_unique_id].keys()):  # means duplicated request
                if self.__g_dictPluginThread[s_plugin_unique_id][s_plugin_name].b_run:
                    self.print_msg_socket(s_plugin_name + ' is already in progress!')
                    self.print_msg_socket(str(self.__g_dictPluginThread[s_plugin_unique_id][s_plugin_name].b_run))
                    return
        try:
            oJobPlugin = importlib.import_module('svplugins.' + s_plugin_name + '.task')
            with oJobPlugin.svJobPlugin() as oJob: # to enforce each plugin follow strict guideline or remove from scheduler
                self.print_msg_socket(s_plugin_name + ' has been initiated')  # oJob.__class__.__name__ + '
                oJob.set_websocket_output(self.print_msg_socket)
                oJob.set_my_name(s_plugin_name)
                oJob.parse_command(lst_command)
                o_plugin_thread = ThreadWithTrace(target=oJob.do_task, args=(self.__cb_thread_done,))
                self.print_msg_socket('begin - ignite')
                o_plugin_thread.start()
                self.print_msg_socket('end - ignite')
        except Exception as err: # prevent websocket disconnection
            self.print_msg_socket(str(err))
        # except ImportError as err:
        #     self.print_msg_socket(self.__get_plugin_instruction('module'))

        if self.__g_dictPluginThread.get(s_plugin_unique_id, self.__g_sSvNull) != self.__g_sSvNull:  # if brand thread exists
            self.__g_dictPluginThread[s_plugin_unique_id][s_plugin_name] = o_plugin_thread
        else:  # if new brand thread requested
            self.__g_dictPluginThread[s_plugin_unique_id] = {s_plugin_name: o_plugin_thread}
        self.__g_dictPluginThread[s_plugin_unique_id][s_plugin_name].b_run = True

    def __cb_thread_done(self, s_plugin_name):
        self.print_msg_socket('begin __cb_thread_done:' + s_plugin_name)
        # s_brand_name = self.scope["url_route"]["kwargs"]["brand_name"]
        sv_acct_id = self.scope["url_route"]["kwargs"]["sv_acct_id"]
        sv_brand_id = self.scope["url_route"]["kwargs"]["sv_brand_id"]
        s_plugin_unique_id = sv_acct_id + '_' + sv_brand_id
        try:
            self.__g_dictPluginThread[s_plugin_unique_id][s_plugin_name].b_run = False
            self.print_msg_socket('end __cb_thread_done:' + s_plugin_name)
            self.print_msg_socket(s_plugin_name + ' has been finished')
        except KeyError:  # plugin name not specified
            self.__halt_all_thread(s_plugin_unique_id)
    
    def __halt_all_thread(self, s_plugin_unique_id=None):
        if s_plugin_unique_id:
            self.print_msg_socket('An weird error occured\nTrying to halt all taks for ' + str(s_plugin_unique_id))
            for s_plugin_name, _ in self.__g_dictPluginThread[s_plugin_unique_id].items():
                self.__g_dictPluginThread[s_plugin_unique_id][s_plugin_name].b_run = False
                self.print_msg_socket(s_plugin_name + ' has been finished')
        else:
            self.print_msg_socket('Trying to halt all taks')
            for s_plugin_unique_id, dict_plugin_thread in self.__g_dictPluginThread.items():
                for s_plugin_name, _ in dict_plugin_thread.items():
                    self.__g_dictPluginThread[s_plugin_unique_id][s_plugin_name].b_run = False
                    self.print_msg_socket(s_plugin_name + ' has been finished')

    def __validate_plugin(self):
        """ find the module in /svplugins directory """
        s_plugin_path_abs = os.path.join(self.__g_sPluginPathAbs, 'task.py')
        if os.path.isfile(s_plugin_path_abs):
            return True
        else:
            return False

    def __get_plugin_instruction(self, s_content):
        """ find the README.md in plugin directory """
        dict_content = {'module': 'IMPORT_MODULE.md'}
        s_plugin_path_abs = os.path.join(self.__g_sPluginPathAbs, 'docs', dict_content[s_content])
        if os.path.isfile(s_plugin_path_abs):
            with open(s_plugin_path_abs, mode='r', encoding='utf-8') as f:
                s_content = f.readlines()
            return ''.join(s_content)
        else:
            return 'uninstalled module detected'
