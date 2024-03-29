"""
Django settings for svdjango project.

Generated by 'django-admin startproject' using Django 3.1.6.

For more information on this file, see
https://docs.djangoproject.com/en/3.1/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/3.1/ref/settings/
"""

import socket  # allow server ip automatically
from pathlib import Path
import os.path  # do not import os
# https://simpleisbetterthancomplex.com/2015/11/26/package-of-the-week-python-decouple.html
from decouple import config, Csv  # https://pypi.org/project/python-decouple/

# to override django.contrib.admin.AdminSite.get_app_list
from django.contrib import admin

import pymysql  # added for nginx
pymysql.install_as_MySQLdb()  # added for nginx

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# allow server IP
ALLOWED_HOSTS = ['127.0.0.1', socket.gethostbyname(socket.getfqdn())]
# allow designated server domain name
lst_domain_name = config('ALLOWED_HOSTS', cast=Csv())  # cast=lambda v: [s.strip() for s in v.split(',')])
for s_domain_name in lst_domain_name:
    if len(s_domain_name):
        ALLOWED_HOSTS.append(s_domain_name)

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/3.1/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = config('SECRET_KEY')

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = config('DEBUG', cast=bool)  # True

# Application definition
INSTALLED_APPS = [
    'channels',  # web socket - # pip install -U channels
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'widget_tweaks',  # for login template
    'crispy_forms',  # for bundle upload form
    'svdaemon.apps.SvdaemonConfig',
    'svload.apps.SvloadConfig',
    'svextract.apps.SvextractConfig',
    'svupload.apps.SvuploadConfig',  # for template
    'svauth.apps.SvauthConfig',  # for user model customization
    'svacct.apps.SvacctConfig',
    'django.contrib.humanize',  # for number formatter on template
]

AUTH_USER_MODEL = 'svauth.User'

# to prevent WARNINGS on Django >= 3.2
# Auto-created primary key used when not defining a primary key type, by default 'django.db.models.AutoField'.
# https://uiandwe.tistory.com/1304
DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'

# Channels
ASGI_APPLICATION = 'svdjango.routing.application'

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'svdjango.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [os.path.join(BASE_DIR, 'templates')],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'svdjango.wsgi.application'

# Database
# https://docs.djangoproject.com/en/3.1/ref/settings/#databases
DATABASES = {
    'default': {
        # 'ENGINE': 'django.db.backends.sqlite3',
        # 'NAME': BASE_DIR / 'db.sqlite3',
        'ENGINE': 'django.db.backends.mysql',
        'CONN_MAX_AGE': 3600,  # https://stackoverflow.com/questions/26958592/django-after-upgrade-mysql-server-has-gone-away
        'NAME': config('db_database'),
        'USER': config('db_userid'),
        'PASSWORD': config('db_password'),
        'HOST': config('db_hostname'),
        'PORT': config('db_port'),
        'OPTIONS': {
            'init_command': "SET sql_mode='STRICT_TRANS_TABLES';",
	    'charset': 'utf8',
        },
    }
}

# Password validation
# https://docs.djangoproject.com/en/3.1/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/3.1/topics/i18n/

LANGUAGE_CODE = 'ko-kr'

TIME_ZONE = 'Asia/Seoul'

USE_I18N = True

USE_L10N = True

USE_TZ = True

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/3.1/howto/static-files/
STATIC_URL = '/static/'
# actual location of static files
STATIC_ROOT = os.path.join(BASE_DIR, 'static_temp')  # activate for [python manage.py collectstatic] only
STATICFILES_DIRS = [os.path.join(BASE_DIR, 'static')]  # 이 리스트에 STATIC_ROOT에서 정의한 디렉토리가 포함되면 안됨

SV_STORAGE_ROOT = os.path.join(BASE_DIR, 'storage')  # forbid http download

# To access the MEDIA_URL in template you must add django.template.context_processors.media to your context_processeors inside the TEMPLATES config.
# this will be deprecated
MEDIA_URL = '/media/'
MEDIA_ROOT = os.path.join(BASE_DIR, 'media')

# LOGIN_URL = '/accounts/login' # keep default
# LOGOUT_REDIRECT_URL = '' # keep default
LOGIN_REDIRECT_URL = '/'

# set admin apps & models ordering list
# https://stackoverflow.com/questions/398163/ordering-admin-modeladmin-objects
ADMIN_ORDERING = [
    ('svacct', [
        'Account',
        'Brand',
        'DataSource',
        'DataSourceDetail',
        'MediaAgency',
        'Contract',
    ]),
    ('svauth', [
        'User',
    ]),
    ('svdaemon', [
        'Job',
    ]),
]
# create an admin apps & models ordering function
def get_app_list_4_admin(self, request):
    dict_app = self._build_app_dict(request)
    for s_app_name, lst_model in ADMIN_ORDERING:
        dict_single_app = dict_app[s_app_name]
        dict_single_app['models'].sort(key=lambda x: lst_model.index(x['object_name']))
        yield dict_single_app

# replace get_app_list() function
admin.AdminSite.get_app_list = get_app_list_4_admin
