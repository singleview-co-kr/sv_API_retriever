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
#from os import listdir
from django.views.generic import TemplateView
from django.shortcuts import render
from django.utils.safestring import mark_safe
from django.contrib.auth.mixins import LoginRequiredMixin

# singleview library

# singleview config
from decouple import config

# def room(request, plugin_name):
#     return render(request, 'svextract/plugin.html', {
#         'plugin_name_json': mark_safe(json.dumps(plugin_name))
#     })

# Create your views here.
class SvPluginWebConsole(LoginRequiredMixin, TemplateView):
    # template_name = 'analyze/index.html'
    __g_sAbsPathBot = None

    def __init__(self):
        self.__g_sAbsPathBot = config('ABSOLUTE_PATH_BOT')
        return

    def get(self, *args, **kwargs):
        s_brand_name = kwargs['brand_name'].strip()

        if not self.request.user.is_authenticated:
            return render(self.request, 'svextract/invalid_user.html', {
                'brand_name_json': mark_safe(s_brand_name)
            })

        if not self.__validate_storage(self.request.user.pk, s_brand_name):
            return render(self.request, 'svextract/invalid_dir.html', {
                'brand_name_json': mark_safe(s_brand_name)
            })
        lst_plugin = self.__get_plugin_lst()
        return render(self.request, 'svextract/plugin.html', {
            'brand_name_json': mark_safe(s_brand_name),
            'lst_plugin': lst_plugin
        })

    def post(self, request, *args, **kwargs):
        s_brand_name = kwargs['brand_name'].strip()

        # create a data storage directory
        s_brand_path_abs = os.path.join(self.__g_sAbsPathBot, 'files', str(self.request.user.pk), s_brand_name)
        if os.path.isdir(s_brand_path_abs) == False:
            os.makedirs(s_brand_path_abs)

        return render(request, 'svextract/plugin.html', {
            'brand_name_json': mark_safe(s_brand_name)
        })

    def __validate_storage(self, n_user_pk, s_brand_name_json):
        """ find the brand designated directory """
        s_brand_path_abs = os.path.join(self.__g_sAbsPathBot, 'files', str(n_user_pk), s_brand_name_json)
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
