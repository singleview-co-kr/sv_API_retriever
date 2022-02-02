import json
import urllib.parse
import mimetypes
import os.path  # do not import os
# from django.conf import settings
from django.shortcuts import render
from django.shortcuts import redirect
from django.views.generic import TemplateView
from django.http import HttpResponse
from django.contrib.auth.mixins import LoginRequiredMixin

# singleview library
from svacct.ownership import get_owned_brand_list
from svcommon import sv_storage
from svcommon import sv_mysql

# Create your views here.
class UploadFileListView(LoginRequiredMixin, TemplateView):
    # template_name = 'analyze/index.html'
    def __init__(self):
        self.__g_oSvStorage = sv_storage.SvStorage()
        self.__g_oSvMysql = sv_mysql.SvMySql()
        return

    def get(self, request, *args, **kwargs):
        dict_rst = get_brand_info(request, kwargs, self.__g_oSvStorage, self.__g_oSvMysql)
        if dict_rst['b_err']:
            dict_context = {'err_msg': dict_rst['s_msg']}
            return render(request, "svupload/upload_deny.html", context=dict_context)
        # s_acct_id = dict_rst['dict_ret']['s_acct_id']
        # s_acct_ttl = dict_rst['dict_ret']['s_acct_ttl']
        s_brand_name = dict_rst['dict_ret']['s_brand_name']
        s_brand_id = dict_rst['dict_ret']['s_brand_id']
        lst_owned_brand = dict_rst['dict_ret']['lst_owned_brand']
        del dict_rst

        lst_files = self.__g_oSvMysql.executeQuery('getUploadedFileAll')
        dict_context = {'s_brand_name': s_brand_name, 'n_brand_id': s_brand_id,
                        'lst_owned_brand': lst_owned_brand, 'lst_files': lst_files}
        return render(request, 'svupload/upload.html', context=dict_context)

    def post(self, request, *args, **kwargs):
        dict_rst = get_brand_info(request, kwargs, self.__g_oSvStorage, self.__g_oSvMysql)
        if dict_rst['b_err']:
            dict_context = {'err_msg': dict_rst['s_msg']}
            return render(request, "svupload/upload_deny.html", context=dict_context)
        # s_acct_id = dict_rst['dict_ret']['s_acct_id']
        # s_acct_ttl = dict_rst['dict_ret']['s_acct_ttl']
        # s_brand_name = dict_rst['dict_ret']['s_brand_name']
        s_brand_id = dict_rst['dict_ret']['s_brand_id']
        del dict_rst

        if request.FILES.get('myfile', None) is None:
            dict_context = {'err_msg': 'no file attached'}
            return render(request, "svupload/upload_deny.html", context=dict_context)

        s_uploaded_filename = request.FILES['myfile'].name
        o_file = request.FILES['myfile']
        dict_rst = self.__g_oSvStorage.register_uploaded_file('upload', s_uploaded_filename, o_file)
        if dict_rst['b_err']:
            dict_context = {'err_msg': dict_rst['s_msg']}
            return render(request, "svupload/upload_deny.html", context=dict_context)
        # begin - file registration into db
        self.__g_oSvMysql.executeQuery('insertUploadedFile', request.user.id, 
                                        dict_rst['dict_val']['s_original_file_name'],
                                        dict_rst['dict_val']['s_file_ext'],
                                        dict_rst['dict_val']['s_secured_file_name'], '')
        # end - file registration into db
        del dict_rst
        return redirect('svupload:index', int(s_brand_id))


class DownloadFileView(LoginRequiredMixin, TemplateView):
    def __init__(self):
        self.__g_oSvStorage = sv_storage.SvStorage()
        self.__g_oSvMysql = sv_mysql.SvMySql()
        return

    def get(self, request, *args, **kwargs):
        dict_rst = get_brand_info(request, kwargs, self.__g_oSvStorage, self.__g_oSvMysql)
        if dict_rst['b_err']:
            dict_context = {'err_msg': dict_rst['s_msg']}
            return render(request, "svupload/download_deny.html", context=dict_context)
        del dict_rst
        lst_single_file = self.__g_oSvMysql.executeQuery('getUploadedFileById', kwargs['n_file_id'])
        dict_rst = self.__g_oSvStorage.get_file('upload', lst_single_file[0]['secured_filename'])
        if dict_rst['b_err']:
            dict_context = {'err_msg': dict_rst['s_msg']}
            return render(request, "svupload/download_deny.html", context=dict_context)
        s_filepath_abs = dict_rst['dict_val']['s_storage_path_abs']
        s_original_filename = lst_single_file[0]['source_filename'] + '.' +  lst_single_file[0]['file_ext']
        del dict_rst
        del lst_single_file
        # https://haandol.wordpress.com/2013/09/10/%ED%95%9C%EA%B8%80%EB%AA%85-%EC%B2%A8%EB%B6%80%ED%8C%8C%EC%9D%BC%EC%9D%84-%EA%B0%95%EC%A0%9C-%EB%8B%A4%EC%9A%B4%EB%A1%9C%EB%93%9C%EC%8B%9C%ED%82%A4%EA%B8%B0/
        with open(s_filepath_abs, 'rb') as fp:
            response = HttpResponse(fp.read())
        content_type, encoding = mimetypes.guess_type(s_original_filename)
        if content_type is None:
            content_type = 'application/octet-stream'
        response['Content-Type'] = content_type
        response['Content-Length'] = str(os.stat(s_filepath_abs).st_size)
        if encoding is not None:
            response['Content-Encoding'] = encoding
        if u'MSIE' in request.META.get('HTTP_USER_AGENT', u'MSIE'):
            filename_header = ''
        elif u'WebKit' in request.META.get('HTTP_USER_AGENT', u'Webkit'):
            # filename_header = 'filename="%s"' % s_original_filename.encode('utf-8')
            filename_header = 'filename*=UTF-8\'\'%s' % urllib.parse.quote(s_original_filename.encode('utf-8'))
        else:
            filename_header = 'filename*=UTF-8\'\'%s' % urllib.parse.quote(s_original_filename.encode('utf-8'))
        response['Content-Disposition'] = 'attachment; ' + filename_header
        return response


class AjaxHandling(LoginRequiredMixin, TemplateView):
    # context_object_name = 'UnzippedFiles'
    template_name = None

    def __init__(self):
        self.__g_oSvStorage = sv_storage.SvStorage()
        self.__g_oSvMysql = sv_mysql.SvMySql()
        return

    def post(self, request):
        # template에서 ajax.POST로 전달
        n_id_brand = request.POST.get('id_brand', None)
        n_id_upload_file = request.POST.get('id_upload_file', None)
        s_req_mode = request.POST.get('req_mode', None)

        print(n_id_brand)
        print(n_id_upload_file)
        print(s_req_mode)
        
        # context를 json 타입으로
        dict_context = {'b_success': 0,  # false
                        'message': '통신 실패',
                        'href': ''}
        kwargs = {'sv_brand_id': n_id_brand}
        dict_rst = get_brand_info(request, kwargs, self.__g_oSvStorage, self.__g_oSvMysql)
        if dict_rst['b_err']:
            dict_context['message'] = dict_rst['s_msg']
            return HttpResponse(json.dumps(dict_context), content_type="application/json")
        del dict_rst

        lst_single_file = self.__g_oSvMysql.executeQuery('getUploadedFileById', n_id_upload_file)
        if lst_single_file[0]['owner_id'] == request.user.id:
            if not request.user.is_admin:
                dict_context['message'] = 'you don\'t own the file you request.'
                return HttpResponse(json.dumps(dict_context), content_type="application/json")

        # # basically, check an upload file level progress status
        # if o_uploaded_file.status != ProgressStatus.UPLOADED and o_uploaded_file.status != ProgressStatus.DENIED:
        #     if not request.user.is_admin:
        #         context['message'] = 'uploaded_file is out of status'
        #         return HttpResponse(json.dumps(context), content_type="application/json")
        #     else:
        #         if o_uploaded_file.status == ProgressStatus.TRANSFORMED:
        #             context['message'] = 'edi_file has been transformed'
        #             return HttpResponse(json.dumps(context), content_type="application/json")

        # # secondly, check an each edi file progress status
        # qs_edi_file = EdiFile.objects.filter(uploaded_file=n_pk_upload_file).filter(owner=self.request.user)
        # # Model.filter() returns queryset; n_owner_id = qs_uploaded_file.values('owner')[0]['owner']
        # for o_edi_file in qs_edi_file:
        #     if o_edi_file.status != ProgressStatus.UPLOADED:
        #         if not request.user.is_admin:
        #             context['message'] = 'edi_file is out of status'
        #             return HttpResponse(json.dumps(context), content_type="application/json")

        # # finally, check an each edi file data type and req_mode
        if s_req_mode == 'delete':
            dict_rst = self.__g_oSvStorage.withdraw_file('upload', lst_single_file[0]['secured_filename'])
            if dict_rst['b_err']:
                dict_context['message'] = dict_rst['s_msg']
                return HttpResponse(json.dumps(dict_context), content_type="application/json")
            self.__g_oSvMysql.executeQuery('updateUploadedFileDeletedById', n_id_upload_file)
        #     for o_edi_file in qs_edi_file:
        #         if o_edi_file.edi_data_type == EdiDataType.ESTIMATION:
        #             if not request.user.is_admin:
        #                 context['message'] = 'edi_file is on estimation'
        #                 return HttpResponse(json.dumps(context), content_type="application/json")
        #     o_uploaded_file.delete()
        # elif s_req_mode == 'update':
        #     for o_edi_file in qs_edi_file:
        #         if o_edi_file.edi_data_type == EdiDataType.ESTIMATION:
        #             if not request.user.is_admin:
        #                 context['message'] = 'edi_file is on estimation'
        #                 return HttpResponse(json.dumps(context), content_type="application/json")
        #     self.__update_edi_file(request)
        # elif s_req_mode == 'transform':  # 모든 edi 파일이 QTY 혹은 AMNT로 결정되어야 진행 시작
        #     n_sv_brand_id = request.POST.get('sv_brand_id', None)
        #     dict_kwargs = {'sv_brand_id': int(n_sv_brand_id)}
        #     dict_acct_rst = get_acct_info(request, dict_kwargs)
        #     del dict_kwargs
        #     for o_edi_file in qs_edi_file:
        #         if o_edi_file.edi_data_type != EdiDataType.QTY_AMNT and \
        #                 o_edi_file.edi_data_type != EdiDataType.QTY and \
        #                 o_edi_file.edi_data_type != EdiDataType.AMNT and \
        #                 o_edi_file.edi_data_type != EdiDataType.IGNORE:
        #             context['message'] = 'undecided edi_file exists'
        #             return HttpResponse(json.dumps(context), content_type="application/json")
        #         if o_edi_file.edi_data_year == 0:
        #             context['message'] = 'edi_file data year is not confirmed'
        #             return HttpResponse(json.dumps(context), content_type="application/json")
        #     self.__transform_edi_file(request, dict_acct_rst, o_uploaded_file)
        #     context['message'] = 'transforming launched!'
        #     context['href'] = resolve_url('svtransform:on_transforming', sv_brand_id=n_sv_brand_id, pk=n_pk_upload_file)
        del lst_single_file
        dict_context['b_success'] = 1  # true
        return HttpResponse(json.dumps(dict_context), content_type="application/json")

    def __update_edi_file(self, request):
        # edi 파일 수정
        lst_edifile_id = request.POST.getlist('arr_edifile_id[]')  # need [] if array transmitted via ajax
        lst_hypermart_title = request.POST.getlist('arr_hypermart_title[]')
        lst_edi_data_year = request.POST.getlist('arr_edi_data_year[]')
        lst_edi_data_type = request.POST.getlist('arr_edi_data_type[]')
        for n_edifile_id in lst_edifile_id:
            o_edi_file = EdiFile.objects.get(id=n_edifile_id)
            o_edi_file.hyper_mart = lst_hypermart_title.pop(0)
            o_edi_file.edi_data_year = lst_edi_data_year.pop(0)
            o_edi_file.edi_data_type = lst_edi_data_type.pop(0)
            o_edi_file.save()
        return

    def __transform_edi_file(self, request, dict_acct_rst, o_uploaded_file):
        # launch pre-process
        # https://stackoverrun.com/ko/q/1880252 <- long running task
        # https://stackoverflow.com/questions/25570910/executing-two-tasks-at-the-same-time-with-celery
        # celery -A svcommon worker -l INFO <- activate celery server
        s_tbl_prefix = dict_acct_rst['dict_ret']['s_tbl_prefix']
        if not s_tbl_prefix:
            dict_context = {'err_msg': 'your analytical context is not defined. plz contact system admin'}
            return render(request, "svtransform/transform_deny.html", context=dict_context)
        # transfer status to ProgressStatus.ON_TRANSFORMING
        o_uploaded_file.status = ProgressStatus.ON_TRANSFORMING
        o_uploaded_file.save()
        # for sv_sql_raw tester
        # from svcommon.sv_sql_raw import SvSqlAccess
        # oDb = SvSqlAccess()
        # oDb.set_tbl_prefix(s_analytical_namespace)
        # oDb.set_app_name(__name__)
        # oDb.initialize()
        # lst_rst = oDb.executeQuery('getEmartExistingLog', 1, 1, 20190103)
        # print(lst_rst)
        dict_param = {
            's_tbl_prefix': s_tbl_prefix,
            'n_owner_id': o_uploaded_file.owner.id,
            'n_req_upload_file_id': request.POST['pk_upload_file']
        }
        # https://stackoverflow.com/questions/47979831/how-to-use-priority-in-celery-task-apply-async
        from .tasks import transform_edi
        transform_edi.apply_async([dict_param], queue='celery', priority=0)
        # for TransferEdiExcelToDb tester
        # from .tasks import TransferEdiExcelToDb
        # o_edi_excel_handler = TransferEdiExcelToDb()
        # o_edi_excel_handler.initialize(dict_param)
        # o_edi_excel_handler.transfer_excel_to_csv()
        # o_edi_excel_handler.transform_csv_to_db()
        print('transform_edi_file done')
        # return HttpResponseRedirect('/redirect/')

def get_brand_info(request, kwargs, o_sv_storage, o_sv_db=None):
    dict_rst = {'b_err': True, 's_msg': None, 'dict_ret': None}
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
        dict_rst['b_err'] = True
        dict_rst['s_msg'] = 'not allowed brand'
        return dict_rst

    o_sv_storage.init(s_acct_id, s_brand_id)
    dict_rst_storage = o_sv_storage.validate('upload')
    if dict_rst_storage['b_err']:
        dict_rst['b_err'] = True
        dict_rst['s_msg'] = dict_rst_storage['s_msg']
        del dict_rst_storage
        return dict_rst
    # s_storage_path_abs = dict_rst_storage['dict_val']['s_storage_path_abs']
    del dict_rst_storage

    if o_sv_db is not None: 
        o_sv_db.setTablePrefix(s_acct_id+'_'+s_brand_id)
        o_sv_db.set_app_name(__name__)
        o_sv_db.initialize({'n_acct_id':int(s_acct_id), 'n_brand_id': int(s_brand_id)})

    dict_rst['b_err'] = False
    dict_rst['s_msg'] = None
    dict_rst['dict_ret'] =  {'s_acct_id': s_acct_id, 's_acct_ttl': s_acct_ttl,
                            's_brand_id': s_brand_id, 's_brand_name': s_brand_name,
                            # 's_storage_path_abs': s_storage_path_abs,
                            'lst_owned_brand': lst_owned_brand}
    return dict_rst
