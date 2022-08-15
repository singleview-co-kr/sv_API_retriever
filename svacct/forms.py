import os
from django import forms
from django.conf import settings
from .models import DataSourceDetail
from svcommon import sv_agency_info


class DataSourceDetailForm(forms.ModelForm):
    # https://stackoverflow.com/questions/17948018/add-custom-form-fields-that-are-not-part-of-the-model-django
    # https://stackoverflow.com/questions/6164773/django-adminform-field-default-value

    # typical agency info: 20220807-20220807\tsingleview\t15%\tmarkup
    s_agency_name = forms.CharField(max_length=25, label="최종 대행사명")
    s_begin_date = forms.CharField(max_length=8, label="최종 대행 시작일 yyyymmdd")
    n_fee_percent = forms.IntegerField(label="최종 수수료%")
    s_fee_type = forms.ChoiceField()
    
    def __init__(self, *arg, **kwargs):
        # make extra field read-only
        # https://stackoverflow.com/questions/67555787/django-forms-how-to-make-certain-fields-readonly-except-for-superuser
        super(DataSourceDetailForm, self).__init__(*arg, **kwargs)
        # self.fields['s_begin_date'].widget.attrs['disabled'] = 'disabled'
        self.fields['s_begin_date'].widget.attrs['readonly'] = True
        o_sv_agency_info = sv_agency_info.SvAgencyInfo()
        lst_type = o_sv_agency_info.get_agency_fee_type()
        o_data_source_detail = kwargs['instance']

        s_acct_pk = str(o_data_source_detail.sv_data_source.sv_brand.sv_acct.pk)
        s_brand_pk = str(o_data_source_detail.sv_data_source.sv_brand.pk)
        s_data_source = str(o_data_source_detail.sv_data_source)
        s_data_source_id = str(o_data_source_detail.s_data_source_serial)
        o_sv_agency_info = sv_agency_info.SvAgencyInfo()
        # s_agency_info_abs_path = os.path.join(settings.SV_STORAGE_ROOT, s_acct_pk, s_brand_pk, s_data_source, s_data_source_id, 'conf', 'agency_info.tsv')
        # o_sv_agency_info.load_agency_info_file(s_agency_info_abs_path)
        o_sv_agency_info.load_agency_info_by_source_id(s_acct_pk, s_brand_pk, s_data_source, s_data_source_id)

        dict_agency_info = o_sv_agency_info.get_latest_agency_info_dict()
        del o_sv_agency_info

        lst_tup_type = []
        for s_type in lst_type:
            lst_tup_type.append((s_type, s_type))
        del lst_type
        self.fields['s_fee_type'] = forms.ChoiceField(
            choices=lst_tup_type,
            label="최종 수수료 유형",
            initial=dict_agency_info['s_fee_type']
        )

    def save(self, commit=True):
        # ...do something with extra_field here...
        s_data_source_serial_id = self.cleaned_data.get('s_data_source_serial', None)
        o_data_source_detail = DataSourceDetail.objects.get(s_data_source_serial=s_data_source_serial_id)
        s_acct_pk = str(o_data_source_detail.sv_data_source.sv_brand.sv_acct.pk)
        s_brand_pk = str(o_data_source_detail.sv_data_source.sv_brand.pk)
        s_data_source = str(o_data_source_detail.sv_data_source)
        s_data_source_id = str(o_data_source_detail.s_data_source_serial)
        del o_data_source_detail

        o_sv_agency_info = sv_agency_info.SvAgencyInfo()
        o_sv_agency_info.load_agency_info_by_source_id(s_acct_pk, s_brand_pk, s_data_source, s_data_source_id)
        # s_agency_info_abs_path = os.path.join(settings.SV_STORAGE_ROOT, s_acct_pk, s_brand_pk, s_data_source, s_data_source_id, 'conf', 'agency_info.tsv')
        # o_sv_agency_info.load_agency_info_file(s_agency_info_abs_path)

        s_agency_name = self.cleaned_data.get('s_agency_name', None)
        s_begin_date = self.cleaned_data.get('s_begin_date', None)
        n_fee_percent = self.cleaned_data.get('n_fee_percent', None)
        s_fee_type = self.cleaned_data.get('s_fee_type', None)
        
        b_rst = o_sv_agency_info.set_agency_info(s_begin_date, s_agency_name, n_fee_percent, s_fee_type)
        del o_sv_agency_info

        return super(DataSourceDetailForm, self).save(commit=commit)

    class Meta:
        model = DataSourceDetail
        fields = '__all__'
        