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

# refer to https://github.com/google/google-api-python-client/tree/master/samples/analytics
# you firstly need to install by cmd "pip3.6 install --upgrade google-api-python-client"
# refer to https://developers.google.com/analytics/devguides/config/mgmt/v3/quickstart/installed-py
# to create desinated credential refer to https://console.developers.google.com/apis/credentials
# to get console credential you firstly need to run with the option --noauth_local_webserver 
# to monitor API traffic refer to https://console.developers.google.com/apis/api/analytics.googleapis.com/quotas?project=svgastudio

# standard library
import os
from django.views.generic import TemplateView
from django.shortcuts import render
from django.utils.safestring import mark_safe
from django.contrib.auth.mixins import LoginRequiredMixin
from django.conf import settings

# singleview library
from svacct.ownership import get_owned_brand_list

# singleview config
from decouple import config


# Create your views here.
class SvPluginWebConsole(LoginRequiredMixin, TemplateView):
    # template_name = 'analyze/index.html'
    __g_sAbsPathBot = None

    def __init__(self):
        self.__g_sAbsPathBot = config('ABSOLUTE_PATH_BOT')
        return

    def get(self, request, *args, **kwargs):
        dict_rst = self._get_brand_info(request, kwargs)
        if dict_rst['b_error']:
            dict_context = {'err_msg': dict_rst['s_msg']}
            return render(request, "svextract/extract_deny.html", context=dict_context)
        s_acct_id = dict_rst['dict_ret']['s_acct_id']
        s_acct_ttl = dict_rst['dict_ret']['s_acct_ttl']
        s_brand_name = dict_rst['dict_ret']['s_brand_name']
        s_brand_id = dict_rst['dict_ret']['s_brand_id']
        if not self.__validate_storage(s_acct_id, s_brand_id):
            dict_context = {'err_msg': '저장소 오류'}
            return render(request, "svextract/extract_deny.html", context=dict_context)

        # lst_owned_brand = dict_rst['dict_ret']['lst_owned_brand']  # for global navigation
        lst_plugin = self.__get_plugin_lst()
        return render(self.request, 'svextract/plugin.html', {
            'acct_ttl': mark_safe(s_acct_ttl),
            'sv_acct_id_json': mark_safe(s_acct_id),
            'brand_name': mark_safe(s_brand_name),
            'sv_brand_id_json': mark_safe(s_brand_id),
            'lst_plugin': lst_plugin
        })

    def __validate_storage(self, s_acct_id, s_brand_id):
        """ find the brand designated directory """
        s_brand_path_abs = os.path.join(self.__g_sAbsPathBot, settings.SV_STORAGE_ROOT, str(s_acct_id), str(s_brand_id))
        if os.path.isdir(s_brand_path_abs):
            return True
        else:
            return False

    def __get_plugin_lst(self):
        """ get modules in /svplugins directory """
        s_plugin_path_abs = os.path.join(self.__g_sAbsPathBot, 'svplugins')
        lst_plugin = [f for f in os.listdir(s_plugin_path_abs) if not f.startswith('_')]
        lst_plugin.append('stop')
        return lst_plugin

    def _get_brand_info(self, request, kwargs):
        dict_rst = {'b_error': False, 's_msg': None, 'dict_ret': None}
        dict_owned_brand = get_owned_brand_list(request, kwargs)
        s_acct_id = None
        s_acct_ttl = None
        s_brand_id = None
        s_brand_name = None
        lst_owned_brand = []
        for _, dict_single_acct in dict_owned_brand.items():
            lst_owned_brand += dict_single_acct['lst_brand']
            for dict_single_brand in dict_single_acct['lst_brand']:
                if dict_single_brand['b_current_brand']:
                    s_acct_id = str(dict_single_acct['n_acct_id'])
                    s_acct_ttl = dict_single_acct['s_acct_ttl']
                    s_brand_id = str(dict_single_brand['n_brand_id'])
                    s_brand_name = dict_single_brand['s_brand_ttl']
                    break

        if not s_acct_id or not s_brand_id:
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = 'not allowed brand'
            return dict_rst

        s_brand_root_abs_path = os.path.join(settings.SV_STORAGE_ROOT, s_acct_id, s_brand_id)
        if not os.path.isdir(s_brand_root_abs_path):  # key.config.ini not exist
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = 'invalid storage'
            return dict_rst

        dict_rst['s_msg'] = 'success'
        dict_rst['dict_ret'] = {'s_acct_id': s_acct_id, 's_acct_ttl': s_acct_ttl,
                                's_brand_id': s_brand_id,
                                's_brand_name': s_brand_name, 'lst_owned_brand': lst_owned_brand}
        return dict_rst
