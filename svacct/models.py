from django.db import models
import re


# Create your models here.
class Account(models.Model):
    s_acct_title = models.CharField(max_length=100, unique=True, verbose_name='구좌 명칭')
    b_approval = models.BooleanField(default=False, verbose_name='승인 여부')
    date_reg = models.DateTimeField(auto_now_add=True, verbose_name='등록일시')

    def __str__(self):
        return self.s_acct_title

    # @admin.display(
    #     boolean=True,
    #     ordering='pub_date',
    #     description='Published recently?',
    # )
    # def was_published_recently(self):
    #     now = timezone.now()
    #     return now - datetime.timedelta(days=1) <= self.pub_date <= now
    # was_published_recently.boolean = True
    # was_published_recently.admin_order_field = '-pub_date'
    # was_published_recently.short_description = 'Published recently?'


class Brand(models.Model):
    sv_acct = models.ForeignKey(Account, on_delete=models.CASCADE, verbose_name='구좌 명칭')
    s_brand_title = models.CharField(max_length=100, verbose_name='브랜드 명칭')
    b_approval = models.BooleanField(default=False, verbose_name='승인 여부')
    date_reg = models.DateTimeField(auto_now_add=True, verbose_name='등록일시')

    class Meta:
        unique_together = ('sv_acct', 's_brand_title',)

    def __str__(self):
        return self.s_brand_title


class DataSourceType(models.IntegerChoices):
    """ data source choices """
    UNDECIDED = 0, '선택하세요'
    GOOGLE_ANALYTICS = 1, '구글 애널리틱스'
    ADWORDS = 2, '구글 광고'  # will be changed to GOOGLE_ADS
    FB_BIZ = 3, '페이스북 광고'
    NAVER_AD = 4, '네이버 광고'
    KAKAO = 5, '카카오 모멘트'

    @classmethod
    def validate_source_id(cls, n_idx, s_id):
        b_rst = False
        if n_idx == cls.GOOGLE_ANALYTICS or n_idx == cls.FB_BIZ or n_idx == cls.NAVER_AD or n_idx == cls.KAKAO:
            b_rst = s_id.isdigit()
        elif n_idx == cls.ADWORDS:
            p = re.compile('\d{3}(-\d{3})(-\d{4})$')  # 123-456-7890
            m = p.match(s_id)
            if m:
                b_rst = True
        return b_rst


class DataSource(models.Model):
    sv_brand = models.ForeignKey(Brand, on_delete=models.CASCADE, verbose_name='브랜드 명칭')
    n_data_source = models.IntegerField(choices=DataSourceType.choices, default=DataSourceType.UNDECIDED, null=False,
                                        verbose_name='데이터 소스')
    b_approval = models.BooleanField(default=False, verbose_name='승인 여부')
    date_reg = models.DateTimeField(auto_now_add=True, verbose_name='등록일시')

    class Meta:
        unique_together = ('sv_brand', 'n_data_source',)

    def __str__(self):
        return DataSourceType(self.n_data_source).name.lower()

    def validate_source_id(self, s_id):
        return DataSourceType.validate_source_id(self.n_data_source, s_id)


class DataSourceDetail(models.Model):
    sv_data_source = models.ForeignKey(DataSource, on_delete=models.CASCADE, verbose_name='데이터 소스')
    s_data_source_serial = models.CharField(max_length=50, verbose_name='데이터 소스 일련번호')
    b_approval = models.BooleanField(default=False, verbose_name='승인 여부')
    date_reg = models.DateTimeField(auto_now_add=True, verbose_name='등록일시')

    class Meta:
        unique_together = ('sv_data_source', 's_data_source_serial',)

    def __str__(self):
        return self.s_data_source_serial
