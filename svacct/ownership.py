from svacct.models import Brand
from svacct.models import Account


def get_owned_brand_list(request, kwargs=None):
    if kwargs:
        n_cur_brand_id = int(kwargs['sv_brand_id'])
    else:
        n_cur_brand_id = -1234  # sentinel value

    try:
        qd_owned_brands = request.user.brand_owners.through.objects.all()
    except AttributeError:
        qd_owned_brands = []
    dict_owned_account = {}
    for o_brand in qd_owned_brands:
        try:
            o_brand_owned = Brand.objects.get(pk=o_brand.brand_id)
        except Brand.DoesNotExist:
            o_brand_owned = None
        if o_brand_owned:
            o_account = Account.objects.get(pk=o_brand_owned.sv_acct_id)
            b_current_brand = True if n_cur_brand_id == o_brand_owned.pk else False
            if dict_owned_account.get(o_brand_owned.sv_acct_id, None):  # if acct exists
                dict_owned_account[o_brand_owned.sv_acct_id]['lst_brand'].append(
                    {'s_brand_ttl': str(o_brand_owned), 'n_brand_id': o_brand_owned.pk, 'b_current_brand': b_current_brand}
                )
            else:
                dict_owned_account[o_brand_owned.sv_acct_id] = {
                    'n_acct_id': o_brand_owned.sv_acct_id, 's_acct_ttl': str(o_account),
                    'lst_brand': [{'s_brand_ttl': str(o_brand_owned), 'n_brand_id': o_brand_owned.pk,
                                   'b_current_brand': b_current_brand}]
                }
            # lst_owned_brand.append({'s_acct_ttl': str(o_account), 'n_acct_id': o_brand_owned.sv_acct_id,
            #                         's_brand_ttl': str(o_brand_owned), 'n_brand_id': o_brand_owned.pk,
            #                         'b_current_brand': b_current_brand})
    return dict_owned_account
