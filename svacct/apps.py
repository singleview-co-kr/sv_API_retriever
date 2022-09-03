from django.apps import AppConfig


class SvacctConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'svacct'

    def ready(self):  # to trigger signals.py
        import svacct.signals
