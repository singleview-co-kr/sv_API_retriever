# from django.shortcuts import render
# for account related
from django.views.generic import CreateView, TemplateView
from django.urls import reverse_lazy
from .forms import UserCreationForm


AppVersion = '0.0.3'
LastModifiedDate = '7th, Jul 2021'


# Create your views here.
# begin - account self service related
class UserCreateView(CreateView):
    template_name = 'registration/register.html'
    form_class = UserCreationForm
    success_url = reverse_lazy('register_done')


class UserCreateDoneTV(TemplateView):
    template_name = 'registration/register_done.html'
# end - account self service related
