#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys

def main():
    """Run administrative tasks."""
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'svdjango.settings')
    from svinitialize import sv_console_init
    o_sv_console_init = sv_console_init.svInitialize(sys.argv)
    s_msg = o_sv_console_init.execute()
    del o_sv_console_init
    if s_msg != 'pass':
        sys.exit(0)

    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()
