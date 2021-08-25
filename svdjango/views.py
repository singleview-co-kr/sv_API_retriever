from django.views.generic import TemplateView
from django.shortcuts import render

# for access control related
from django.contrib.auth.mixins import AccessMixin
# from svauth.forms import UserCreationForm

AppVersion = '0.0.13'
LastModifiedDate = '20th, Jun 2021'


#def index(request):
#    return render(request, 'chat/index.html', {})

class IndexView(TemplateView):
    template_name = 'index.html'


# for access control related
class OwnerOnlyMixin(AccessMixin):
    raise_exception = True
    permission_denied_message = 'Owner only can modify the object'

    def get(self, request, *args, **kwargs):
        obj = self.get_object()
        if request.user != obj.owner:
            return self.handle_no_permission()
        return super().get(request, *args, **kwargs)
