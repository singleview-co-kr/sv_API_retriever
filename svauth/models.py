from django.db import models
from django.contrib.auth.models import BaseUserManager, AbstractBaseUser


# Create your models here.
# https://dev-yakuza.posstree.com/ko/django/custom-user-model/
# https://beomi.github.io/2017/02/02/Django-CustomAuth/
# https://velog.io/@kineqwer1123/Django-User-Model-%ED%99%95%EC%9E%A5%ED%95%98%EA%B8%B0
# https://docs.djangoproject.com/en/3.1/topics/auth/customizing/
# https://simpleisbetterthancomplex.com/tutorial/2016/07/22/how-to-extend-django-user-model.html
class UserManager(BaseUserManager):  # 유저를 생성할 때 사용하는 헬퍼(Helper) 클래스
    def create_user(self, email, company_name, password=None):
        if not email:
            raise ValueError('Users must have an email address')

        user = self.model(
            email=self.normalize_email(email),
            company_name=company_name,
        )

        user.set_password(password)
        user.save(using=self._db)
        return user

    def create_superuser(self, email, company_name, password):
        user = self.create_user(
            email,
            password=password,
            company_name=company_name,
        )
        user.is_admin = True
        user.save(using=self._db)
        return user


class User(AbstractBaseUser):  # 실제 User Model
    email = models.EmailField(
        verbose_name='email',
        max_length=255,
        unique=True,
    )
    company_name = models.CharField(max_length=255)
    analytical_namespace = models.CharField(max_length=10)
    is_active = models.BooleanField(default=True)
    is_admin = models.BooleanField(default=False)

    objects = UserManager()

    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = ['company_name']

    def __str__(self):
        return self.email

    def has_perm(self, perm, obj=None):
        return True

    def has_module_perms(self, app_label):
        return True

    @property
    def is_staff(self):
        return self.is_admin
