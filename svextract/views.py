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

# singleview library

# singleview config
from conf import basic_config

# def room(request, plugin_name):
#     return render(request, 'svextract/plugin.html', {
#         'plugin_name_json': mark_safe(json.dumps(plugin_name))
#     })

# Create your views here.
class SvPluginWebConsole(TemplateView):
    # template_name = 'analyze/index.html'

    def __init__(self):
        return

    def get(self, request, *args, **kwargs):
        s_brand_name = kwargs['brand_name'].strip()

        #print(request.user.pk)
        if not request.user.is_authenticated:
            # dict_rst['b_error'] = True
            # dict_rst['s_msg'] = 'user not logged id'
            # return dict_rst
            print('denied')
        # print(request.user.analytical_namespace)

        if not self.__validate_storage(request.user.pk, s_brand_name):
            return render(request, 'svextract/invalid_dir.html', {
                'brand_name_json': mark_safe(s_brand_name)
            })

        #if not self.__validate_plugin(s_plugin_name):
        #    s_plugin_name = 'invalid plugin'

        return render(request, 'svextract/plugin.html', {
            'brand_name_json': mark_safe(s_brand_name)
        })

    def post(self, request, *args, **kwargs):
        s_brand_name = kwargs['brand_name'].strip()

        print(self.request.user)
        print(s_brand_name)

        s_brand_path_abs = os.path.join(basic_config.ABSOLUTE_PATH_BOT, 'files', str(self.request.user.pk), s_brand_name)
        if os.path.isdir(s_brand_path_abs) == False:
            os.makedirs(s_brand_path_abs)

        return render(request, 'svextract/plugin.html', {
            'brand_name_json': mark_safe(s_brand_name)
        })

        # pre-process
        
        
        # dict_param = dict_rst['dict_param']
        # # begin - uploading file registration
        # n_edi_yr_from_upload_filename = dict_param['n_edi_yr_from_upload_filename']
        # # https://wayhome25.github.io/django/2017/04/01/django-ep9-crud/
        # for s_unzipped_file in dict_param['lst_unzipped_files']:
        #     o_edi_file = EdiFile(hyper_mart=HyperMartType.ESTIMATION, edi_data_year=n_edi_yr_from_upload_filename,
        #                             owner=self.request.user, edi_file_name=s_unzipped_file,
        #                             uploaded_file=o_new_uploaded_file)
        #     o_edi_file.save()
        # # end - uploading file registration
        # from .tasks import lookup_edi_file
        # dict_param['s_tbl_prefix'] = self.request.user.analytical_namespace
        # if len(dict_param['lst_unzipped_files']) > 2:
        #     lookup_edi_file.apply_async([dict_param], queue='celery', priority=10)  # , countdown=10)
        #     # lookup_edi_file(dict_param)
        # else:
        #     lookup_edi_file(dict_param)

        # return redirect('svtransform:index')

    def __validate_storage(self, n_user_pk, s_brand_name_json):
        """ find the brand designated directory """
        s_brand_path_abs = os.path.join(basic_config.ABSOLUTE_PATH_BOT, 'files', str(n_user_pk), s_brand_name_json)
        if os.path.isdir(s_brand_path_abs):
            return True
        else:
            return False

    # def __validate_plugin(self, s_plugin_name):
    #     """ find the module in /svplugins directory """
    #     s_plugin_path_abs = os.path.join(basic_config.ABSOLUTE_PATH_BOT, 'svplugins', s_plugin_name, 'task.py')

    #     if os.path.isfile(s_plugin_path_abs):
    #         return True
    #     else:
    #         return False
