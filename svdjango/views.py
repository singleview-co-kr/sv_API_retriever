from django.views.generic import TemplateView
from django.shortcuts import render

# for access control related
from django.contrib.auth.mixins import AccessMixin
from svacct.ownership import get_owned_brand_list


#def index(request):
#    return render(request, 'chat/index.html', {})

class IndexView(TemplateView):
    def get(self, request, *args, **kwargs):
        dict_owned_brand = get_owned_brand_list(request)
        if request.user.is_authenticated:
            s_index_html = 'index_member.html'
        else:
            s_index_html = 'index_guest.html'
        return render(request, s_index_html, {'dict_owned_brand': dict_owned_brand})


# for access control related
class OwnerOnlyMixin(AccessMixin):
    raise_exception = True
    permission_denied_message = 'Owner only can modify the object'

    def get(self, request, *args, **kwargs):
        obj = self.get_object()
        if request.user != obj.owner:
            return self.handle_no_permission()
        return super().get(request, *args, **kwargs)
