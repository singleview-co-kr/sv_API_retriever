from django import forms
# from django.contrib import messages
from django.core.exceptions import ValidationError

from .models import Job
from .models import TriggerType

g_lst_interval_trigger = ['weeks', 'days', 'hours', 'minutes', 'seconds']
g_lst_cron_trigger = ['year', 'month', 'week', 'day', 'day_of_week', 'hour', 'minute', 'second']


class JobAdminForm(forms.ModelForm):
    # https://stackoverflow.com/questions/6821161/django-admin-return-custom-error-message-during-model-saving
    class Meta:
        model = Job
        fields = ['sv_brand', 's_trigger_type', 's_trigger_params']

    def clean(self):
        cleaned_data = self.cleaned_data
        pk_sv_acct = cleaned_data.get('sv_acct').pk
        fk_sv_brand = cleaned_data.get('sv_brand').sv_acct_id
        if pk_sv_acct != fk_sv_brand:
            error_message = str(cleaned_data.get('sv_brand')) + ' is not belonged to ' + \
                            str(cleaned_data.get('sv_acct'))
            field = 'sv_brand'
            self.add_error(field, error_message)
            raise ValidationError(error_message)

        s_trigger_type = cleaned_data.get('s_trigger_type')
        s_trigger_params = cleaned_data.get('s_trigger_params')
        if s_trigger_type == TriggerType.INTERVAL:
            for s_line in s_trigger_params.splitlines():
                lst_param = s_line.split('=')
                if lst_param[0] not in g_lst_interval_trigger:
                    error_message = 'only ' + ', '.join(g_lst_interval_trigger) + ' params are allowed if you chose ' + \
                                    TriggerType.INTERVAL.label
                    field = 's_trigger_params'
                    self.add_error(field, error_message)
                    raise ValidationError(error_message)
        elif s_trigger_type == TriggerType.CRON:
            for s_line in s_trigger_params.splitlines():
                lst_param = s_line.split('=')
                if lst_param[0] not in g_lst_cron_trigger:
                    error_message = 'only ' + ', '.join(g_lst_cron_trigger) + ' params are allowed if you chose ' + \
                                    TriggerType.CRON.label
                    field = 's_trigger_params'
                    self.add_error(field, error_message)
                    raise ValidationError(error_message)
        return self.cleaned_data
