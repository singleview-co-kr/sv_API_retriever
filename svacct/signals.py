from django.db.models.signals import post_save
from django.dispatch import receiver
from .models import Contract
from svcommon import sv_agency_info


@receiver(post_save, sender=Contract)
def contract_post_save(sender, instance, created, **kwargs):
    # https://dgkim5360.tistory.com/entry/django-signal-example
    # https://stackoverflow.com/questions/61184031/post-save-signal-for-specific-profile-model-for-user-registration
    lst_contracts = []
    n_datasource_id = instance.sv_data_source.pk
    qs_contracts = sender.objects.filter(sv_data_source=n_datasource_id).order_by('pk')
    for o_single_contract in qs_contracts:
        lst_contracts.append({
            'date_begin': o_single_contract.date_begin,
            'date_end': o_single_contract.date_end,
            'media_agency_id': o_single_contract.sv_media_agency.pk,
            'n_agent_fee_percent': o_single_contract.n_agent_fee_percent,
            's_fee_type': o_single_contract.s_fee_type,
        })
    del o_single_contract
    del qs_contracts
    s_acct_pk = str(instance.sv_data_source.sv_data_source.sv_brand.sv_acct.pk)
    s_brand_pk = str(instance.sv_data_source.sv_data_source.sv_brand.pk)
    s_data_source = str(instance.sv_data_source.sv_data_source)
    s_data_source_id = str(instance.sv_data_source.s_data_source_serial)
    o_sv_agency_info = sv_agency_info.SvAgencyInfo()
    o_sv_agency_info.load_by_source_id(s_acct_pk, s_brand_pk, s_data_source, s_data_source_id)
    o_sv_agency_info.set_agency_info(lst_contracts)
    del o_sv_agency_info
