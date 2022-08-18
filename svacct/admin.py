import os
import csv
from pathlib import Path
from datetime import datetime
from django.contrib import messages
from django.contrib import admin
from django.contrib.admin import display  # was introduced in Django 3.2
from django.conf import settings

from svcommon import sv_agency_info

from .models import Account
from .models import Brand
from .models import DataSource
from .models import DataSourceType
from .models import DataSourceDetail
from .forms import DataSourceDetailForm

from django.template.response import TemplateResponse
from django.urls import path


# Register your models here.
class MyModelAdmin(admin.ModelAdmin):
    def get_urls(self):
        urls = super().get_urls()
        my_urls = [
            path('my_view/', self.my_view),
        ]
        return my_urls + urls

    def my_view(self, request):
        # ...
        context = dict(
           # Include common variables for rendering the admin template.
           self.admin_site.each_context(request),
           # Anything else you want in the context...
           key='value',
        )
        return TemplateResponse(request, "sometemplate.html", context)

class BrandInline(admin.TabularInline):
    model = Brand
    extra = 2

    def save_model(self, request, obj, form, change):
        pass  # print('ddd')


# https://show-me-the-money.tistory.com/entry/Django-Admin%EC%97%90%EC%84%9C-Form-%EC%A0%80%EC%9E%A5-%EC%BB%A4%EC%8A%A4%ED%84%B0%EB%A7%88%EC%9D%B4%EC%A7%95%ED%95%98%EA%B8%B0
class AccountAdmin(admin.ModelAdmin):
    fieldsets = [  # choose editable attr
        (None, {'fields': ['s_acct_title']}),
        # ('Date information', {'fields': ['date_reg'], 'classes': ['collapse']}),
    ]
    inlines = [BrandInline]
    list_display = ('s_acct_title', 'date_reg')
    list_filter = ['date_reg']
    search_fields = ['s_acct_title']

    def save_model(self, request, obj, form, change):
        # https://devnauts.tistory.com/197
        obj.user = request.user
        # self.message_user(request, 'dsafasdf', level=messages.ERROR)
        # # You can also use warning, debug, info and success in place of error
        super().save_model(request, obj, form, change)
        # print(obj.pk)
        s_acct_root_abs_path = os.path.join(settings.SV_STORAGE_ROOT, str(obj.pk))
        if not os.path.isdir(s_acct_root_abs_path):
            os.makedirs(s_acct_root_abs_path)

    def save_formset(self, request, form, formset, change):
        # https://jay-ji.tistory.com/32
        formset.save()
        n_acct_pk = None
        n_brand_pk = None
        if formset.is_valid():
            for form1 in formset:
                if 'sv_acct' in form1.cleaned_data.keys():
                    n_acct_pk = form1.cleaned_data['sv_acct'].pk
                if 's_brand_title' in form1.cleaned_data.keys():
                    if not form1.cleaned_data['DELETE']:
                        try:
                            o_brand_appended = Brand.objects.get(s_brand_title=form1.cleaned_data['s_brand_title'])
                        except Brand.DoesNotExist:
                            o_brand_appended = None
                        n_brand_pk = o_brand_appended.pk
                if n_acct_pk and n_brand_pk:
                    s_brand_root_abs_path = os.path.join(settings.SV_STORAGE_ROOT, str(n_acct_pk), str(n_brand_pk))
                    if not os.path.isdir(s_brand_root_abs_path):
                        os.makedirs(s_brand_root_abs_path)

    def delete_model(self, request, obj):
        obj.user = request.user
        super().delete_model(request, obj)


class DataSourceInline(admin.TabularInline):
    # https://stackoverflow.com/questions/63916655/how-can-i-access-attributes-of-a-model-in-admin-tabularinline-that-is-at-the-end
    model = DataSource
    extra = 2
    readonly_fields = ['date_reg', ]


class BrandAdmin(admin.ModelAdmin):
    # fieldsets = [  # choose editable attr
    #     (None, {'fields': ['s_brand_title']}),
    #     # ('Date information', {'fields': ['date_reg'], 'classes': ['collapse']}),
    # ]
    # readonly_fields = ['s_brand_title', ]
    inlines = [DataSourceInline]
    list_display = ('sv_acct', 's_brand_title', 'date_reg')
    list_filter = ['date_reg']
    search_fields = ['sv_acct__s_acct_title', 's_brand_title']

    def get_readonly_fields(self, request, obj=None):
        if obj: # make a field read-only if edit an existing object
            return self.readonly_fields + ('s_brand_title', )
        return self.readonly_fields  # but required when adding new obj

    def save_model(self, request, obj, form, change):
        obj.user = request.user
        # s_base_dir = Path(__file__).resolve().parent.parent
        # print(form.cleaned_data)
        super().save_model(request, obj, form, change)
        # print(obj.pk)
        s_acct_root_abs_path = os.path.join(settings.SV_STORAGE_ROOT, str(obj.pk))
        if not os.path.isdir(s_acct_root_abs_path):
            os.makedirs(s_acct_root_abs_path)

    def save_formset(self, request, form, formset, change):
        # https://jay-ji.tistory.com/32
        formset.save()
        if formset.is_valid():
            n_acct_pk = None
            n_brand_pk = None
            n_data_source_pk = None
            # print(formset.cleaned_data)
            for form1 in formset:
                # print(form1.cleaned_data)
                if 'sv_brand' in form1.cleaned_data.keys():
                    n_acct_pk = form1.cleaned_data['sv_brand'].sv_acct_id
                    n_brand_pk = form1.cleaned_data['sv_brand'].pk
                    if not form1.cleaned_data['DELETE']:
                        try:
                            o_data_source_appended = DataSource.objects.get(sv_brand=n_brand_pk, n_data_source=form1.cleaned_data['n_data_source'])
                        except Brand.DoesNotExist:
                            o_data_source_appended = None
                        n_data_source_pk = o_data_source_appended.pk
                        if n_acct_pk and n_brand_pk and n_data_source_pk and n_data_source_pk != 0:
                            s_data_source_root_abs_path = os.path.join(settings.SV_STORAGE_ROOT, str(n_acct_pk),
                                                                       str(n_brand_pk), str(o_data_source_appended))
                            if not os.path.isdir(s_data_source_root_abs_path):
                                os.makedirs(s_data_source_root_abs_path)

    def delete_model(self, request, obj):
        obj.user = request.user
        super().delete_model(request, obj)


# Register your models here.
class DataSourceIdInline(admin.TabularInline):
    model = DataSourceDetail
    extra = 1
    # readonly_fields = ['s_data_source_id', ]


class DataSourceAdmin(admin.ModelAdmin):
    # fieldsets = [  # choose editable attr
    #     (None, {'fields': ['s_brand_title']}),
    #     # ('Date information', {'fields': ['date_reg'], 'classes': ['collapse']}),
    # ]
    # readonly_fields = ['n_data_source', ]
    inlines = [DataSourceIdInline]
    list_display = ('get_account', 'sv_brand', 'n_data_source', 'date_reg')
    list_filter = ['date_reg']
    search_fields = ['sv_brand__sv_acct__s_acct_title', 'sv_brand__s_brand_title', 'n_data_source']

    # https://stackoverflow.com/questions/163823/can-list-display-in-a-django-modeladmin-display-attributes-of-foreignkey-field
    @display(ordering='sv_brand__sv_acct', description='구좌 명칭')
    def get_account(self, obj):
        return obj.sv_brand.sv_acct

    def get_readonly_fields(self, request, obj=None):
        if obj: # make a field read-only if edit an existing object
            return self.readonly_fields + ('n_data_source', )
        return self.readonly_fields  # but required when adding new obj

    def save_model(self, request, obj, form, change):
        obj.user = request.user
        s_base_dir = Path(__file__).resolve().parent.parent
        super().save_model(request, obj, form, change)

    def save_formset(self, request, form, formset, change):
        # https://jay-ji.tistory.com/32
        if formset.is_valid():
            n_acct_id = None
            n_brand_pk = None
            o_data_source_id = None
            for form1 in formset:
                # print(form1.cleaned_data)
                if 'sv_data_source' in form1.cleaned_data.keys():
                    n_brand_pk = form1.cleaned_data['sv_data_source'].sv_brand_id  # 48
                    if not form1.cleaned_data['DELETE']:
                        s_data_source_serial_id = form1.cleaned_data['s_data_source_serial']
                        # print(form1.cleaned_data['sv_data_source'].validate_source_id(s_data_source_serial_id))
                        if form1.cleaned_data['sv_data_source'].validate_source_id(s_data_source_serial_id):
                            try:
                                o_brand = Brand.objects.get(pk=n_brand_pk)
                            except Brand.DoesNotExist:
                                o_brand = None
                            n_acct_id = o_brand.sv_acct_id
                            o_data_source_id = form1.cleaned_data['sv_data_source']

                            if n_acct_id and n_brand_pk and o_data_source_id and o_data_source_id != 0 \
                                    and len(s_data_source_serial_id):
                                s_data_source_id_abs_path = os.path.join(settings.SV_STORAGE_ROOT, str(n_acct_id),
                                                                         str(n_brand_pk), str(o_data_source_id),
                                                                         s_data_source_serial_id)
                                if not os.path.isdir(s_data_source_id_abs_path):
                                    os.makedirs(s_data_source_id_abs_path)
                                    os.makedirs(os.path.join(s_data_source_id_abs_path, 'conf'))  # make conf folder
                                    os.makedirs(os.path.join(s_data_source_id_abs_path, 'data'))  # make data folder
                                # proc source - fb biz 
                                if form1.cleaned_data['sv_data_source'].n_data_source == DataSourceType.FB_BIZ:
                                    try:
                                        with open(os.path.join(s_data_source_id_abs_path, 'conf', 'info_fx.tsv'), "w") as o_file:
                                            o_file.write("KRW") 
                                    except PermissionError:
                                        pass
                        else:
                            self.message_user(request, '형식에 맞지 않는 데이터 소스 일련번호입니다. 삭제해 주세요.', level=messages.ERROR)
                    formset.save()

    def delete_model(self, request, obj):
        obj.user = request.user
        super().delete_model(request, obj)


class DataSourceDetailAdmin(admin.ModelAdmin):
    # set extra form
    form = DataSourceDetailForm

    fieldsets = [  # choose editable attr
        (None, {'fields': ['s_data_source_serial', 'sv_data_source', 's_agency_name', 's_begin_date',
                            'n_fee_percent', 's_fee_type']}),
        # ('Date information', {'fields': ['date_reg'], 'classes': ['collapse']}),
    ]
    # readonly_fields = ['sv_data_source', ]
    list_display = ('get_account', 'get_brand', 'sv_data_source', 's_data_source_serial', )
    list_filter = ['date_reg']
    search_fields = ['sv_data_source__sv_brand__sv_acct__s_acct_title', 'sv_data_source__sv_brand__s_brand_title',
                     'sv_data_source__n_data_source', 's_data_source_serial']
    
    # https://stackoverflow.com/questions/163823/can-list-display-in-a-django-modeladmin-display-attributes-of-foreignkey-field
    @display(ordering='sv_brand__sv_acct', description='구좌 명칭')
    def get_account(self, obj):
        return obj.sv_data_source.sv_brand.sv_acct

    @display(ordering='sv_data_source__sv_brand', description='브랜드 명칭')
    def get_brand(self, obj):
        return obj.sv_data_source.sv_brand

    def get_readonly_fields(self, request, obj=None):
        if obj: # make a field read-only if edit an existing object
            return self.readonly_fields + ('sv_data_source', )
        return self.readonly_fields  # but required when adding new obj

    def get_form(self, request, obj=None, **kwargs):
        # https://stackoverflow.com/questions/6164773/django-adminform-field-default-value
        form = super(DataSourceDetailAdmin, self).get_form(request, obj, **kwargs)
        s_acct_pk = str(obj.sv_data_source.sv_brand.sv_acct.pk)
        s_brand_pk = str(obj.sv_data_source.sv_brand.pk)
        s_data_source = str(obj.sv_data_source)
        s_data_source_id = str(obj.s_data_source_serial)
        o_sv_agency_info = sv_agency_info.SvAgencyInfo()
        o_sv_agency_info.load_agency_info_by_source_id(s_acct_pk, s_brand_pk, s_data_source, s_data_source_id)
        dict_agency_info = o_sv_agency_info.get_latest_agency_info_dict()
        if dict_agency_info['s_agency_name'] != '' and dict_agency_info['s_fee_type'] != '':
            form.base_fields['s_agency_name'].initial = dict_agency_info['s_agency_name']
            form.base_fields['s_begin_date'].initial = dict_agency_info['s_begin_date']
            form.base_fields['n_fee_percent'].initial = dict_agency_info['n_fee_rate']
            form.base_fields['s_fee_type'].initial = dict_agency_info['s_fee_type'] 
        else:
            form.base_fields['s_begin_date'].initial = str(datetime.today().strftime('%Y%m%d'))
        del dict_agency_info
        del o_sv_agency_info
        return form


admin.site.register(Account, AccountAdmin)
admin.site.register(Brand, BrandAdmin)
admin.site.register(DataSource, DataSourceAdmin)
admin.site.register(DataSourceDetail, DataSourceDetailAdmin)
