"""
WSGI config for svdjango project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/3.1/howto/deployment/wsgi/
"""

import os

# begin - for python3.7 on centos7
from pathlib import Path 
import sys 
# end - for python3.7 on centos7

from django.core.wsgi import get_wsgi_application

# begin - for python3.7 on centos7
BASE_DIR = str(Path(__file__).resolve().parent.parent)
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)
# end - for python3.7 on centos7

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'svdjango.settings')

application = get_wsgi_application()
