# standard library
import os
import importlib
import json
import threading
from svauth.models import User
from channels.generic.websocket import WebsocketConsumer

# singleview config
from conf import basic_config

# install WebSocket
# https://ssungkang.tistory.com/entry/Django-Channels-%EB%B9%84%EB%8F%99%EA%B8%B0%EC%A0%81-%EC%B1%84%ED%8C%85-%EA%B5%AC%ED%98%84%ED%95%98%EA%B8%B0-WebSocket-1
# # pip install -U channels

class PluginConsole(WebsocketConsumer):
    __g_sPluginPathAbs = None
    __g_dictPluginThread = {}  # should improve to a multiuser threading use case

    # websocket 연결 시 실행
    def connect(self):
        # print('ws connect')
        # https://stackoverflow.com/questions/65482182/problem-changing-from-websocketconsumer-to-asyncwebsocketconsumer
        self.user = self.scope['user']
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
        """ 클라이언트로부터 메세지를 받을 시 실행 """
        try:
            qs_owner_info = User.objects.get(id=self.user.pk)
            # print(qs_owner_info)
        except User.DoesNotExist:
            self.print_msg_socket('invalid user')

        text_data_json = json.loads(text_data)
        s_command = text_data_json['command'].strip()

        # 클라이언트로부터 받은 명령어를 다시 클라이언트로 보내준다.
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
                self.__g_dictPluginThread[s_interrupt_plugin_name].b_run = False
                self.print_msg_socket(s_interrupt_plugin_name + ' has been interrupted!')
            except KeyError:
                self.print_msg_socket(s_interrupt_plugin_name + ' is unable to be interrupted - not running')
            return

        # integrate_db mode=ad
        self.__g_sPluginPathAbs = os.path.join(basic_config.ABSOLUTE_PATH_BOT, 'svplugins', s_plugin_name)

        if not self.__validate_plugin():
            self.print_msg_socket('invalid plugin')
            return

        s_analytical_namespace = self.user.analytical_namespace
        if len(s_analytical_namespace) == 0:
            self.print_msg_socket('invalid analytical_namespace')
            return

        lst_command.append('analytical_namespace='+s_analytical_namespace)
        # https://channels.readthedocs.io/en/latest/topics/routing.html
        s_brand_name = self.scope["url_route"]["kwargs"]["brand_name"]
        lst_command.append('config_loc='+str(self.user.pk) + '/' + s_brand_name)

        if s_plugin_name in list(self.__g_dictPluginThread.keys()):  # means duplicated request
            self.print_msg_socket(s_plugin_name + ' is already in progress!')
            return

        try:
            oJobPlugin = importlib.import_module('svplugins.' + s_plugin_name + '.task')
            with oJobPlugin.svJobPlugin() as oJob: # to enforce each plugin follow strict guideline or remove from scheduler
                self.print_msg_socket(s_plugin_name + ' has been initiated')  # oJob.__class__.__name__ + '
                oJob.set_websocket_output(self.print_msg_socket)
                oJob.set_my_name(s_plugin_name)
                oJob.parse_command(lst_command)
                self.__g_dictPluginThread[s_plugin_name] = threading.Thread(target=oJob.do_task, args=(self.__cb_thread_done,))
                self.__g_dictPluginThread[s_plugin_name].b_run = True
                self.__g_dictPluginThread[s_plugin_name].start()
                # self.__g_dictPluginThread[s_plugin_name].join()  # join() means to order to wait for thread completion, not stopping thread
        except Exception as err: # prevent websocket disconnection
            self.print_msg_socket(str(err))
        # except ImportError as err:
        #     self.print_msg_socket(self.__get_plugin_instruction('module'))
    
    def __cb_thread_done(self, s_plugin_name):
        try:
            del self.__g_dictPluginThread[s_plugin_name]
            self.print_msg_socket(s_plugin_name + ' has been finished')
            print(s_plugin_name + ' has been finished')
        except KeyError:  # plugin name not specified
            self.__halt_all_thread()
    
    def __halt_all_thread(self):
        self.print_msg_socket('An weird error occured\nTrying to halt all taks')
        print('An weird error occured\nTrying to halt all taks')
        for s_plugin_name, o_plugin_thread in self.__g_dictPluginThread.items():
            o_plugin_thread.b_run = False
            self.print_msg_socket(s_plugin_name + ' has been finished')
            print(s_plugin_name + ' has been finished')

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
