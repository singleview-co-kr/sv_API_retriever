# Generated by Django 3.2.5 on 2022-09-03 03:50

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('svacct', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='MediaAgency',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('s_agency_name', models.CharField(max_length=25, verbose_name='대행사명')),
                ('s_agency_rep_name', models.CharField(max_length=15, verbose_name='담당자명')),
                ('s_agency_contact', models.CharField(max_length=15, verbose_name='연락처')),
                ('b_approval', models.BooleanField(default=False, verbose_name='승인 여부')),
                ('date_reg', models.DateTimeField(auto_now_add=True, verbose_name='등록일시')),
            ],
        ),
        migrations.CreateModel(
            name='Contract',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('n_agent_fee_percent', models.IntegerField(default=0, verbose_name='수수료율')),
                ('s_fee_type', models.CharField(choices=[('back', '백마진'), ('markup', '마크업')], default='direct', max_length=7, verbose_name='수수료 형태')),
                ('date_begin', models.DateField(blank=True, null=True, verbose_name='대행 시작일')),
                ('date_end', models.DateField(blank=True, null=True, verbose_name='대행 종료일')),
                ('b_approval', models.BooleanField(default=False, verbose_name='승인 여부')),
                ('date_reg', models.DateTimeField(auto_now_add=True, verbose_name='등록일시')),
                ('sv_data_source', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='svacct.datasourcedetail', verbose_name='데이터 소스')),
                ('sv_media_agency', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='svacct.mediaagency', verbose_name='매체 대행사')),
            ],
            options={
                'unique_together': {('sv_data_source', 'sv_media_agency', 'n_agent_fee_percent', 'date_begin', 'date_end')},
            },
        ),
    ]
