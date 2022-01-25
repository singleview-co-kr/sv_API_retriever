import os
from pathlib import Path
from django.contrib import messages
from django.contrib import admin
from django.contrib.admin import display  # was introduced in Django 3.2
from django.conf import settings

from .models import Account
from .models import Brand
from .models import DataSource
from .models import DataSourceDetail


# Register your models here.
class BrandInline(admin.TabularInline):
    model = Brand
    extra = 2

    def save_model(self, request, obj, form, change):
        print('ddd')


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
        print('deleted')
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
        print('deleted')
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
    readonly_fields = ['n_data_source', ]
    inlines = [DataSourceIdInline]
    list_display = ('get_account', 'sv_brand', 'n_data_source', 'date_reg')
    list_filter = ['date_reg']
    search_fields = ['sv_brand__sv_acct__s_acct_title', 'sv_brand__s_brand_title', 'n_data_source']

    # https://stackoverflow.com/questions/163823/can-list-display-in-a-django-modeladmin-display-attributes-of-foreignkey-field
    @display(ordering='sv_brand__sv_acct', description='구좌 명칭')
    def get_account(self, obj):
        return obj.sv_brand.sv_acct

    def save_model(self, request, obj, form, change):
        obj.user = request.user
        # print(request.POST)
        # print(obj)
        s_base_dir = Path(__file__).resolve().parent.parent
        # print(s_base_dir)
        super().save_model(request, obj, form, change)

    def save_formset(self, request, form, formset, change):
        # https://jay-ji.tistory.com/32
        if formset.is_valid():
            n_acct_id = None
            n_brand_pk = None
            n_data_source_id = None
            # print(formset.cleaned_data)
            for form1 in formset:
                # print(form1.cleaned_data)
                if 'sv_data_source' in form1.cleaned_data.keys():
                    # print(form1.cleaned_data['sv_data_source'].sv_brand_id)
                    n_brand_pk = form1.cleaned_data['sv_data_source'].sv_brand_id  # 48
                    # print(n_brand_pk)
                    if not form1.cleaned_data['DELETE']:
                        s_data_source_serial_id = form1.cleaned_data['s_data_source_serial']
                        # print(form1.cleaned_data['sv_data_source'].validate_source_id(s_data_source_serial_id))
                        if form1.cleaned_data['sv_data_source'].validate_source_id(s_data_source_serial_id):
                            try:
                                o_brand = Brand.objects.get(pk=n_brand_pk)
                            except Brand.DoesNotExist:
                                o_brand = None
                            n_acct_id = o_brand.sv_acct_id
                            n_data_source_id = form1.cleaned_data['sv_data_source']
                            if n_acct_id and n_brand_pk and n_data_source_id and n_data_source_id != 0 \
                                    and len(s_data_source_serial_id):
                                s_data_source_id_abs_path = os.path.join(settings.SV_STORAGE_ROOT, str(n_acct_id),
                                                                         str(n_brand_pk), str(n_data_source_id),
                                                                         s_data_source_serial_id)
                                if not os.path.isdir(s_data_source_id_abs_path):
                                    os.makedirs(s_data_source_id_abs_path)
                        else:
                            self.message_user(request, '형식에 맞지 않는 데이터 소스 일련번호입니다. 삭제해 주세요.', level=messages.ERROR)
                    formset.save()

    def delete_model(self, request, obj):
        obj.user = request.user
        print('deleted')
        super().delete_model(request, obj)


class DataSourceDetailAdmin(admin.ModelAdmin):
    # fieldsets = [  # choose editable attr
    #     (None, {'fields': ['s_brand_title']}),
    #     # ('Date information', {'fields': ['date_reg'], 'classes': ['collapse']}),
    # ]
    readonly_fields = ['sv_data_source', ]
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


admin.site.register(Account, AccountAdmin)
admin.site.register(Brand, BrandAdmin)
admin.site.register(DataSource, DataSourceAdmin)
admin.site.register(DataSourceDetail, DataSourceDetailAdmin)
