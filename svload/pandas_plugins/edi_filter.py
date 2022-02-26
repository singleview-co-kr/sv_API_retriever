# for logger
import logging

from svcommon.sv_hypermart_model import BranchType, SvHyperMartType, SvHypermartGeoInfo

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class EdiFilter:
    __g_oHttpRequest = None
    __g_dictBranchInfo = {}
    __g_dictSalesChInfo = None
    __g_dictFilter = {'s_sales_ch_mode': None, 'lst_sales_ch': [], 's_branch_mode': None, 'lst_branch': [],
                      's_sku_mode': None, 'lst_sku': []}

    # def __new__(cls, request):
    #    # print(__file__ + ':' + sys._getframe().f_code.co_name)  # turn to decorator
    #    return super().__new__(cls)

    def __init__(self, request):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        self.__g_oHttpRequest = request

        # begin - set effective hyper mart type dict
        dict_hyper_mart_type = SvHyperMartType.get_dict_by_idx()
        del dict_hyper_mart_type[1]  # remove ESTIMATION
        del dict_hyper_mart_type[2]  # remove NOT_SURE
        del dict_hyper_mart_type[5]  # remove HOMEPLUS
        self.__g_dictSalesChInfo = dict_hyper_mart_type

        # begin - hyper mart branch object
        dict_branch_by_title = SvHyperMartType.get_dict_by_title()
        dict_branch_type = BranchType.get_dict_by_title()
        o_mart_geo_info = SvHypermartGeoInfo()
        for dict_single_branch in o_mart_geo_info.lst_hypermart_geo_info:
            n_hypermart_id = dict_branch_by_title[dict_single_branch['hypermart_name']]
            n_branch_type_id = dict_branch_type[dict_single_branch['branch_type']]
            dict_branch = {'id': dict_single_branch['id'], 'selected': '', 'hypermart_id': n_hypermart_id,
                           'hypermart_name': dict_single_branch['hypermart_name'],
                           'name': dict_single_branch['name'],
                           'branch_type_id': n_branch_type_id,
                           'branch_type': dict_single_branch['branch_type'],
                           'do_name': dict_single_branch['do_name'], 'si_name': dict_single_branch['si_name'],
                           'gu_gun': dict_single_branch['gu_gun'], 'dong_myun_ri': dict_single_branch['dong_myun_ri'],
                           'latitude': dict_single_branch['latitude'], 'longitude': dict_single_branch['longitude']}
            self.__g_dictBranchInfo[dict_single_branch['id']] = dict_branch
        super().__init__()

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __del__(self):
        # logger.debug('__del__')
        pass

    def get_sales_ch(self):
        # sch means sales_ch
        s_sch_filter_mode = ''
        lst_selected_sch = []
        try:
            s_include_sch_id = self.__g_oHttpRequest.GET['sales_ch_inc']
            lst_selected_sch = s_include_sch_id.split(',')
            s_sch_filter_mode = s_sch_filter_mode + 'inc'
        except KeyError:
            pass
        try:
            s_exclude_sch_id = self.__g_oHttpRequest.GET['sales_ch_exc']
            lst_selected_sch = s_exclude_sch_id.split(',')
            s_sch_filter_mode = s_sch_filter_mode + 'exc'
        except KeyError:
            pass

        if s_sch_filter_mode.find('exc') > -1 and s_sch_filter_mode.find('inc') > -1:
            raise Exception('weird sales ch filter')

        # 영업 채널 일련번호를 문자열에서 정수로 변경
        lst_selected_sch = [int(item) for item in lst_selected_sch]
        # begin - set sales ch info for filter UI
        dict_sch_info_for_ui = {}
        for n_idx, single_sch_title in self.__g_dictSalesChInfo.items():
            dict_sch = {'id': n_idx, 'selected': '', 'sales_ch_name': single_sch_title}
            try:
                lst_selected_sch.index(n_idx)
                dict_sch['selected'] = 'selected'
            except ValueError:
                pass
            dict_sch_info_for_ui[n_idx] = dict_sch
        # end - set sales h info for filter UI
        self.__g_dictFilter['s_sales_ch_mode'] = s_sch_filter_mode
        self.__g_dictFilter['lst_sales_ch'] = lst_selected_sch
        return {'dict_sales_ch_info_for_ui': dict_sch_info_for_ui, 's_sales_ch_filter_mode': s_sch_filter_mode}

    def get_branch(self, n_branch_id=None):
        # self.__g_dfEmartBranchId = pd.DataFrame(dict_all_branch_info_by_id).transpose()
        s_branch_filter_mode = ''
        lst_selected_branch = []
        dict_branch_info_for_ui = {}
        if n_branch_id:  # for ByBranchEdi.view()
            dict_single_branch = self.__g_dictBranchInfo[n_branch_id]
            dict_branch_info_for_ui[n_branch_id] = dict_single_branch
            s_branch_filter_mode = 'inc'
            lst_selected_branch.append(n_branch_id)
        else:  # for other view()
            try:
                s_include_branch_id = self.__g_oHttpRequest.GET['branch_inc']
                lst_selected_branch = s_include_branch_id.split(',')
                s_branch_filter_mode = s_branch_filter_mode + 'inc'
            except KeyError:
                pass
            try:
                s_exclude_branch_id = self.__g_oHttpRequest.GET['branch_exc']
                lst_selected_branch = s_exclude_branch_id.split(',')
                s_branch_filter_mode = s_branch_filter_mode + 'exc'
            except KeyError:
                pass

            if s_branch_filter_mode.find('exc') > -1 and s_branch_filter_mode.find('inc') > -1:
                raise Exception('weird branch filter')

            # 매장 일련번호를 문자열에서 정수로 변경
            lst_selected_branch = [int(item) for item in lst_selected_branch]
            # begin - set edi branch info for filter UI
            if self.__g_dictFilter['s_sales_ch_mode'] == 'inc':
                for n_branch_id, dict_single_branch in self.__g_dictBranchInfo.items():
                    if dict_single_branch['hypermart_id'] in self.__g_dictFilter['lst_sales_ch']:
                        try:
                            lst_selected_branch.index(n_branch_id)
                            dict_single_branch['selected'] = 'selected'
                        except ValueError:
                            pass
                        dict_branch_info_for_ui[n_branch_id] = dict_single_branch
            elif self.__g_dictFilter['s_sales_ch_mode'] == 'exc':
                for n_branch_id, dict_single_branch in self.__g_dictBranchInfo.items():
                    if dict_single_branch['hypermart_id'] not in self.__g_dictFilter['lst_sales_ch']:
                        try:
                            lst_selected_branch.index(n_branch_id)
                            dict_single_branch['selected'] = 'selected'
                        except ValueError:
                            pass
                        dict_branch_info_for_ui[n_branch_id] = dict_single_branch
            else:
                for n_branch_id, dict_single_branch in self.__g_dictBranchInfo.items():
                    try:
                        lst_selected_branch.index(n_branch_id)
                        dict_single_branch['selected'] = 'selected'
                    except ValueError:
                        pass
                    dict_branch_info_for_ui[n_branch_id] = dict_single_branch
            # end - set edi branch info for filter UI
        self.__g_dictFilter['s_branch_mode'] = s_branch_filter_mode
        self.__g_dictFilter['lst_branch'] = lst_selected_branch
        return {'dict_branch_info_for_ui': dict_branch_info_for_ui, 's_branch_filter_mode': s_branch_filter_mode}

    def get_sku_by_brand_id(self, o_sv_db, n_sku_id=None):
        """
        :param o_sv_db:
        :param n_sku_id: set only if sku-detail-view
        :return:
        """
        s_sku_filter_mode = ''
        lst_selected_sku = []
        try:
            s_include_sku_id = self.__g_oHttpRequest.GET['sku_inc']
            lst_selected_sku = s_include_sku_id.split(',')
            s_sku_filter_mode = s_sku_filter_mode + 'inc'
        except KeyError:
            pass
        try:
            s_exclude_sku_id = self.__g_oHttpRequest.GET['sku_exc']
            lst_selected_sku = s_exclude_sku_id.split(',')
            s_sku_filter_mode = s_sku_filter_mode + 'exc'
        except KeyError:
            pass

        if s_sku_filter_mode.find('exc') > -1 and s_sku_filter_mode.find('inc') > -1:
            raise Exception('weird sku filter')

        lst_selected_sku_for_ui = []
        if s_sku_filter_mode != '':  # sku filter designated
            lst_selected_sku_for_ui = [int(item) for item in lst_selected_sku]  # SKU 일련번호를 문자열에서 정수로 변경

        lst_sku_rst = o_sv_db.executeQuery('getEdiSkuInfoByAccept', 1)
        lst_selected_sku = []
        # 좁은 범위의 필터부터 살펴봄
        if n_sku_id:  # sku designated
            for dict_single_sku in lst_sku_rst:
                if dict_single_sku['id'] == n_sku_id:
                    lst_selected_sku.append(dict_single_sku)
                    break
        elif self.__g_dictFilter['s_branch_mode']:  # branch designated
            lst_mart_id = []
            for n_branch_id in self.__g_dictFilter['lst_branch']:
                dict_single_branch = self.__g_dictBranchInfo[n_branch_id]
                lst_mart_id.append(dict_single_branch['hypermart_id'])

            if self.__g_dictFilter['s_branch_mode'] == 'inc':
                for dict_single_sku in lst_sku_rst:
                    if dict_single_sku['mart_id'] in lst_mart_id:
                        lst_selected_sku.append(dict_single_sku)
            elif self.__g_dictFilter['s_branch_mode'] == 'exc':
                for dict_single_sku in lst_sku_rst:
                    if dict_single_sku['mart_id'] not in lst_mart_id:
                        lst_selected_sku.append(dict_single_sku)
        elif self.__g_dictFilter['s_sales_ch_mode']:  # sales ch designated
            if self.__g_dictFilter['s_sales_ch_mode'] == 'inc':
                for dict_single_sku in lst_sku_rst:
                    if dict_single_sku['mart_id'] in self.__g_dictFilter['lst_sales_ch']:
                        lst_selected_sku.append(dict_single_sku)
            elif self.__g_dictFilter['s_sales_ch_mode'] == 'exc':
                for dict_single_sku in lst_sku_rst:
                    if dict_single_sku['mart_id'] not in self.__g_dictFilter['lst_sales_ch']:
                        lst_selected_sku.append(dict_single_sku)
        else:  # no filter
            lst_selected_sku = lst_sku_rst
        del lst_sku_rst
        # if self.__g_dictFilter['s_branch_mode'] == 'inc':  # branch designated
        #     lst_mart_id = []
        #     for n_branch_id in self.__g_dictFilter['lst_branch']:
        #         dict_single_branch = self.__g_dictBranchInfo[n_branch_id]
        #         lst_mart_id.append(dict_single_branch['hypermart_id'])
        #     lst_unique_mart_id = [str(n_mart_id) for n_mart_id in list(set(lst_mart_id))]  # for uniqueness
        #     dict_param_tmp = {'n_brand_id': n_brand_id, 's_req_mart_id_set': ','.join(lst_unique_mart_id)}
        #     print(dict_param_tmp)
        #     lst_sku_rst = o_sv_db.executeDynamicQuery('getEdiSkuInfoByBrandBranchId', dict_param_tmp)
        #     del dict_param_tmp
        # elif n_sku_id:  # sku designated
        #     lst_sku_rst = o_sv_db.executeQuery('getEdiSkuInfoByBrandSkuId', n_sku_id)
        #     # {19: {'mart_id': 3, 'mart_name': 'Emart', 'name': '유한락스 욕실청소 900ML*2', 'first_detect_logdate': datetime.date(2019, 1, 1)}}
        # else:  # all branch & all sku mode
        #     lst_sku_rst = o_sv_db.executeQuery('getEdiSkuInfoByBrandId', n_brand_id)
        dict_hyper_mart_type = SvHyperMartType.get_dict_by_idx()
        dict_sku_info_by_id = {}
        if len(lst_selected_sku):
            for dict_sku in lst_selected_sku:
                dict_sku_info_by_id[dict_sku['id']] = {'hypermart_name': self.__g_dictSalesChInfo[dict_sku['mart_id']],
                                                       'selected': '',
                                                       'mart_id': dict_sku['mart_id'],
                                                       # 'mart_name': dict_hyper_mart_type[dict_sku['mart_id']],
                                                       'name': dict_sku['item_name'],
                                                       'first_detect_logdate': dict_sku['first_detect_logdate']}
        del dict_hyper_mart_type

        # begin - set sku info for filter UI
        dict_sku_info_for_ui = {}
        for n_sku_id, dict_single_sku in dict_sku_info_by_id.items():
            # print(dict_single_sku)
            dict_sku = {'id': n_sku_id, 'selected': '', 'sch_name': dict_single_sku['hypermart_name'],
                        'name': dict_single_sku['name']}
            try:
                lst_selected_sku_for_ui.index(n_sku_id)
                dict_sku['selected'] = 'selected'
            except ValueError:
                pass
            dict_sku_info_for_ui[n_sku_id] = dict_sku
        # end - set sku info for filter UI
        self.__g_dictFilter['s_sku_mode'] = s_sku_filter_mode
        self.__g_dictFilter['lst_sku'] = lst_selected_sku

        if s_sku_filter_mode != '':  # sku filter designated
            dict_sku_filtered = {}
            for n_sku_id in lst_selected_sku_for_ui:
                dict_sku_filtered[n_sku_id] = dict_sku_info_by_id[n_sku_id]
            dict_sku_info_by_id = dict_sku_filtered

        return {'dict_sku_info_by_id': dict_sku_info_by_id, 'dict_sku_info_for_ui': dict_sku_info_for_ui,
                's_sku_filter_mode': s_sku_filter_mode}
