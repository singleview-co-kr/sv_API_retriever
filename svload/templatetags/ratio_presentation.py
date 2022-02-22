from django import template  # for user-defined template tag

# https://wayhome25.github.io/django/2017/06/22/custom-template-filter/
register = template.Library()


@register.filter
def growth_rate(n_current, n_prev):
    if n_prev > 0:
        return ((n_current / n_prev) - 1) * 100
    else:
        return 0
