import os
from django.conf import settings
from django.db import models
from django.utils.translation import gettext_lazy as _

from svacct.models import Account
from svacct.models import Brand


# Create your models here.
class TriggerType(models.TextChoices):
    """ trigger type choices """
    NONE = 'n', _('미확정')
    INTERVAL = 'i', _('빈도')
    CRON = 'c', _('일정')
    # models.IntegerChoices
    # NONE = 0, '미확정'
    # INTERVAL = 1, '빈도'
    # CRON = 2, '일정'


def get_active_plugins():
    # https://forum.djangoproject.com/t/how-can-i-implement-a-model-field-with-dynamic-choices/7604
    # https://stackoverflow.com/questions/6001986/dynamic-choices-field-in-django-models
    # https://stackoverflow.com/questions/1325673/how-to-add-property-to-a-class-dynamically
    s_svplugins_root = os.path.join(settings.BASE_DIR, 'svplugins')
    if os.path.isdir(s_svplugins_root):
        lst_svplugins = [f.name for f in os.scandir(s_svplugins_root) if f.is_dir() and f.name != '_blank']
    else:
        lst_svplugins = []
    # lst_temp = [
    #     {'plan_id': 1, 'unique_name': '34'},
    #     {'plan_id': 2, 'unique_name': '56'}
    # ]
    lst_svplugin_choices = [
        # (p["plan_id"], str(p["plan_id"]) + " - " + p["unique_name"])
        (s_plugin, s_plugin) for s_plugin in lst_svplugins
    ]
    return lst_svplugin_choices


class Job(models.Model):
    sv_acct = models.ForeignKey(Account, on_delete=models.CASCADE, verbose_name='구좌 명칭')
    sv_brand = models.ForeignKey(Brand, on_delete=models.CASCADE, verbose_name='브랜드 명칭')
    s_job_title = models.CharField(max_length=255, verbose_name='작업 명칭', null=False)
    date_start = models.DateField(max_length=10, verbose_name='작업 시작일', blank=True, null=True)
    date_end = models.DateField(max_length=10, verbose_name='작업 종료일', blank=True, null=True)
    s_plugin_name = models.CharField(max_length=50, choices=get_active_plugins(), verbose_name='플러그인 명칭', null=False)
    s_plugin_params = models.TextField(verbose_name='플러그인 인수', blank=True, null=True)
    s_trigger_type = models.CharField(max_length=1, choices=TriggerType.choices, default=TriggerType.NONE,
                                      verbose_name='트리거 유형', null=False)
    s_trigger_params = models.TextField(verbose_name='트리거 인수', blank=True, null=True)
    is_active = models.BooleanField(default=False, verbose_name='활성 여부')
    is_deleted = models.BooleanField(default=False, verbose_name='삭제 여부')
    dt_reg = models.DateTimeField(auto_now_add=True, verbose_name='등록일시')
    dt_mod = models.DateTimeField(verbose_name='수정일시', null=True)
    dt_applied = models.DateTimeField(verbose_name='적용일시', null=True)

    class Meta:
        unique_together = ('sv_brand', 's_job_title',)

    def __str__(self):
        return self.s_job_title
