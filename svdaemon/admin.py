from datetime import datetime
from django.contrib import admin

from .models import Job
from .forms import JobAdminForm


# Register your models here.
# class UserChoiceField(ModelChoiceField):
#     def label_from_instance(self, obj):
#         return '%s' % obj.s_brand_title


class JobAdmin(admin.ModelAdmin):
    readonly_fields = ['dt_reg', 'dt_mod', 'dt_applied']
    fieldsets = [  # choose editable attr
        (None, {'fields': ['sv_acct', 'sv_brand', 's_job_title',
                           'date_start', 'date_end',
                           's_plugin_name', 's_plugin_params',
                           's_trigger_type', 's_trigger_params',
                           'is_active', 'is_deleted',
                           'dt_reg', 'dt_mod', 'dt_applied']}),
        # ('Date information', {'fields': ['date_reg'], 'classes': ['collapse']}),
    ]
    form = JobAdminForm

    # def formfield_for_foreignkey(self, db_field, request, **kwargs):
    #     if db_field.name == 'sv_brand':
    #         kwargs['form_class'] = UserChoiceField
    #     return super(JobAdmin, self).formfield_for_foreignkey(db_field, request, **kwargs)

    def save_model(self, request, obj, form, change):
        # https://devnauts.tistory.com/197
        print(form.changed_data)
        if 'sv_acct' in form.changed_data or \
                'sv_brand' in form.changed_data or \
                's_job_title' in form.changed_data or \
                'date_start' in form.changed_data or \
                'date_end' in form.changed_data or \
                's_plugin_name' in form.changed_data or \
                's_plugin_params' in form.changed_data or \
                's_trigger_type' in form.changed_data or \
                's_trigger_params' in form.changed_data or \
                'is_active' in form.changed_data or \
                'is_deleted'  in form.changed_data:
            obj.dt_mod = datetime.now()
        super().save_model(request, obj, form, change)


admin.site.register(Job, JobAdmin)
