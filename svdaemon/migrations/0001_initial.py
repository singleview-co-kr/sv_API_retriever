# Generated by Django 3.2.5 on 2022-05-03 01:21

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('svacct', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='Job',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('s_job_title', models.CharField(max_length=255, verbose_name='작업 명칭')),
                ('date_start', models.DateField(blank=True, max_length=10, null=True, verbose_name='작업 시작일')),
                ('date_end', models.DateField(blank=True, max_length=10, null=True, verbose_name='작업 종료일')),
                ('s_plugin_name', models.CharField(choices=[('aw_get_day', 'aw_get_day'), ('aw_get_month', 'aw_get_month'), ('aw_get_period', 'aw_get_period'), ('aw_register_db', 'aw_register_db'), ('cafe24_register_db', 'cafe24_register_db'), ('client_serve', 'client_serve'), ('edi_register_db', 'edi_register_db'), ('fb_get_day', 'fb_get_day'), ('fb_get_period', 'fb_get_period'), ('fb_register_db', 'fb_register_db'), ('ga_get_day', 'ga_get_day'), ('ga_get_period', 'ga_get_period'), ('ga_register_db', 'ga_register_db'), ('integrate_db', 'integrate_db'), ('kko_register_db', 'kko_register_db'), ('nvad_get_day', 'nvad_get_day'), ('nvad_get_period', 'nvad_get_period'), ('nvad_register_db', 'nvad_register_db'), ('slack_clear', 'slack_clear'), ('collect_twitter', 'collect_twitter'), ('collect_svdoc', 'collect_svdoc'), ('daily_cron', 'daily_cron'), ('collect_svadr', 'collect_svadr')], max_length=50, verbose_name='플러그인 명칭')),
                ('s_plugin_params', models.TextField(blank=True, null=True, verbose_name='플러그인 인수')),
                ('s_trigger_type', models.CharField(choices=[('n', '미확정'), ('i', '빈도'), ('c', '일정')], default='n', max_length=1, verbose_name='트리거 유형')),
                ('s_trigger_params', models.TextField(blank=True, null=True, verbose_name='트리거 인수')),
                ('is_active', models.BooleanField(default=False, verbose_name='활성 여부')),
                ('is_deleted', models.BooleanField(default=False, verbose_name='삭제 여부')),
                ('dt_reg', models.DateTimeField(auto_now_add=True, verbose_name='등록일시')),
                ('dt_mod', models.DateTimeField(null=True, verbose_name='수정일시')),
                ('dt_applied', models.DateTimeField(null=True, verbose_name='적용일시')),
                ('sv_acct', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='svacct.account', verbose_name='구좌 명칭')),
                ('sv_brand', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='svacct.brand', verbose_name='브랜드 명칭')),
            ],
            options={
                'unique_together': {('sv_brand', 's_job_title')},
            },
        ),
    ]