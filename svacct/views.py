from django.shortcuts import render
from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic import TemplateView
from django.db.models.base import ObjectDoesNotExist
from svacct.models import Brand
from svacct.models import Account
from svacct.models import DataSource
from svacct.models import DataSourceDetail
from svacct.ownership import get_owned_brand_list


# Create your views here.
class IndexView(LoginRequiredMixin, TemplateView):
    # template_name = 'analyze/index.html'
    def get(self, request, *args, **kwargs):
        lst_owned_brand = get_owned_brand_list(request)
        return render(request, 'svacct/index.html', {'lst_owned_brand': lst_owned_brand})


class BrandConfView(LoginRequiredMixin, TemplateView):
    # template_name = 'analyze/index.html'
    def get(self, request, *args, **kwargs):
        n_brand_id_arg = kwargs['n_brand_id']
        b_brand_ownership = None
        try:
            request.user.brand_owners.through.objects.get(brand_id=n_brand_id_arg)
            b_brand_ownership = True
        except ObjectDoesNotExist:
            b_brand_ownership = False

        if b_brand_ownership:
            qd_allocated_data_source = DataSource.objects.filter(sv_brand_id=n_brand_id_arg)
            for o_data_source in qd_allocated_data_source:
                # print(o_data_source.pk)
                qd_allocated_data_source_id = DataSourceDetail.objects.filter(sv_data_source_id=o_data_source.pk)
                print(qd_allocated_data_source_id)
        # lst_owned_brand = []
        # for o_brand in qd_owned_brands:
        #     try:
        #         o_brand_owned = Brand.objects.get(pk=o_brand.brand_id)
        #     except Brand.DoesNotExist:
        #         o_brand_owned = None
        #
        #     if o_brand_owned:
        #         o_account = Account.objects.get(pk=o_brand_owned.sv_acct_id)
        #         lst_owned_brand.append({'s_acct_ttl': str(o_account), 'n_acct_id': o_brand_owned.sv_acct_id,
        #                                 's_brand_ttl': str(o_brand_owned), 'n_brand_id': o_brand_owned.pk})
        # print(lst_owned_brand)
        return render(request, 'svacct/brand_conf.html', {'lst_owned_brand': None})
