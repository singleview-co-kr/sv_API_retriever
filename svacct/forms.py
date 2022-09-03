from django import forms
from .models import DataSourceDetail


class DataSourceDetailForm(forms.ModelForm):
    # https://stackoverflow.com/questions/17948018/add-custom-form-fields-that-are-not-part-of-the-model-django
    # https://stackoverflow.com/questions/6164773/django-adminform-field-default-value

    # typical agency info: 20220807-20220807\tsingleview\t15%\tmarkup
    s_acct_name = forms.CharField(max_length=25, label="구좌명")
    s_brand_name = forms.CharField(max_length=25, label="브랜드명")
    n_datasource_id = forms.CharField(max_length=25, label="데이터소스 ID")
    # n_fee_percent = forms.IntegerField(label="최종 수수료%")
    # s_fee_type = forms.ChoiceField()
    
    def __init__(self, *arg, **kwargs):
        super(DataSourceDetailForm, self).__init__(*arg, **kwargs)

        # make extra field read-only
        # https://stackoverflow.com/questions/67555787/django-forms-how-to-make-certain-fields-readonly-except-for-superuser
        # self.fields['s_begin_date'].widget.attrs['disabled'] = 'disabled'

        o_data_source_detail = kwargs['instance']
        self.fields['s_acct_name'].initial = str(o_data_source_detail.sv_data_source.sv_brand.sv_acct)
        self.fields['s_acct_name'].widget.attrs['readonly'] = True
        self.fields['s_brand_name'].initial = str(o_data_source_detail.sv_data_source.sv_brand)
        self.fields['s_brand_name'].widget.attrs['readonly'] = True
        self.fields['n_datasource_id'].initial = o_data_source_detail.pk
        self.fields['n_datasource_id'].widget.attrs['readonly'] = True

        # self.fields['s_fee_type'] = forms.ChoiceField(
        #     choices=lst_tup_type,
        #     label="최종 수수료 유형",
        #     initial=dict_agency_info['s_fee_type']
        # )

    class Meta:
        model = DataSourceDetail
        fields = '__all__'
        