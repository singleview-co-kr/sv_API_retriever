from django import forms
from django.contrib.auth.forms import ReadOnlyPasswordHashField
import random

from .models import User


class UserCreationForm(forms.ModelForm):
    password1 = forms.CharField(label='Password', widget=forms.PasswordInput)
    password2 = forms.CharField(
        label='Password confirmation', widget=forms.PasswordInput)

    class Meta:
        model = User
        fields = ('email', 'company_name')

    def clean_password2(self):
        password1 = self.cleaned_data.get("password1")
        password2 = self.cleaned_data.get("password2")
        if password1 and password2 and password1 != password2:
            raise forms.ValidationError("Passwords don't match")
        return password2

    def save(self, commit=True):
        user = super().save(commit=False)
        user.set_password(self.cleaned_data["password1"])
        user.analytical_namespace = self.__get_unique_namespace()
        if commit:
            user.save()
        return user

    def __get_unique_namespace(self, n_namespace_len=8):
        # set tbl prefix for each account
        if n_namespace_len > 10:
            n_namespace_len = 10  # refer to column max_length
        # s_allowed_chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefghjklmnopqrstuvwxyz_'
        s_allowed_chars = 'abcdefghjklmnopqrstuvwxyz1234567890_'
        # mysql tbl name does not differ upper and lower case
        s_u_namespace = ''.join(random.sample(s_allowed_chars, len(s_allowed_chars)))
        return s_u_namespace[:n_namespace_len]


class UserChangeForm(forms.ModelForm):
    password = ReadOnlyPasswordHashField()

    class Meta:
        model = User
        fields = ('email', 'password', 'company_name', 'is_active', 'is_admin')

    def clean_password(self):
        return self.initial["password"]
