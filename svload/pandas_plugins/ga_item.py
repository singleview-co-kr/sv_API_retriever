import importlib

# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class GaItem:
    """ depends on svplugins.ga_register_db.item_performance """
    # __g_bPeriodDebugMode = False
    __g_oSvDb = None
    __g_sLstIgnoreItemTitle = ['(not set)']
    __g_lstAllowedPattern = ['100', '110', '111']  # 1 means filled catalog hierarchy level

    def __init__(self, o_sv_db):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        self.__g_oSvDb = o_sv_db
        super().__init__()

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __del__(self):
        # logger.debug('__del__')
        del self.__g_oSvDb

    # def activate_debug(self):
    #     self.__g_bPeriodDebugMode = True

    def get_list(self):
        """
        data for item list screen
        """
        dict_arranged_catalog_depth = self.__get_cat_depth_dictionary()
        # begin - construct item list
        lst_cleaned_catalog = []
        lst_rst = self.__g_oSvDb.executeQuery('getGaItemList')
        if len(lst_rst):
            for dict_single_item in lst_rst:
                if dict_single_item['item_title'] in self.__g_sLstIgnoreItemTitle:
                    continue
                else:
                    n_item_srl = dict_single_item['item_srl']
                    if n_item_srl in dict_arranged_catalog_depth:
                        for dict_cat_depth in dict_arranged_catalog_depth[n_item_srl]:
                            s_cat_id = 'cat' + str(dict_cat_depth['cat_depth'])
                            dict_single_item[s_cat_id] = dict_cat_depth['cat_title']
                    lst_cleaned_catalog.append(dict_single_item)
            del dict_single_item
        del dict_arranged_catalog_depth
        del lst_rst
        # end - construct item list
        return {'lst_catalog': lst_cleaned_catalog}

    def update_item(self, request, n_sv_acct_id, n_brand_id):
        """ 
        :param n_sv_acct_id: is to execute the client_serve plugin
        :param n_brand_id: is to execute the client_serve plugin
        """
        lst_item_srl = request.POST.getlist('item_srls[]')
        lst_cat1 = request.POST.getlist('item_cat1[]')
        lst_cat2 = request.POST.getlist('item_cat2[]')
        lst_cat3 = request.POST.getlist('item_cat3[]')
        if len(lst_item_srl) != len(lst_cat1) or len(lst_cat1) != len(lst_cat2) or \
                len(lst_cat2) != len(lst_cat3):
            print('weird data')
            return
        # begin - create effective item cat list
        lst_catalog = []
        n_idx = 0
        for n_item_srl in lst_item_srl:
            s_cat1 = lst_cat1[n_idx].strip()
            s_cat2 = lst_cat2[n_idx].strip()
            s_cat3 = lst_cat3[n_idx].strip()
            b_cat1 = '1' if len(s_cat1) else '0'
            b_cat2 = '1' if len(s_cat2) else '0'
            b_cat3 = '1' if len(s_cat3) else '0'
            if b_cat1+b_cat2+b_cat3 in self.__g_lstAllowedPattern:
                lst_catalog.append({'n_item_srl': n_item_srl, 's_cat1': s_cat1, 
                                    's_cat2': s_cat2, 's_cat3': s_cat3})
            n_idx += 1
        del lst_item_srl
        del lst_cat1
        del lst_cat2
        del lst_cat3
        # end - create effective item cat list

        dict_arranged_catalog_depth = self.__get_cat_depth_dictionary()
        b_changed_something = False  # a flag to 
        for dict_single_item in lst_catalog:
            n_item_srl = None
            lst_cat_depth = None
            for s_key, s_val in dict_single_item.items():
                if s_key == 'n_item_srl':
                    n_item_srl = int(s_val)
                    if n_item_srl in dict_arranged_catalog_depth:
                        lst_cat_depth = dict_arranged_catalog_depth[n_item_srl]
                elif len(s_val):  # fill in some value
                    s_cat_depth = s_key.replace('s_cat', '')
                    # print(s_cat_depth, s_val)
                    b_proc = False
                    if lst_cat_depth is not None:  # update old cat depth info
                        for dict_cat_depth in lst_cat_depth:
                            if s_cat_depth == str(dict_cat_depth['cat_depth']):
                                if dict_cat_depth['cat_title'] != s_val:
                                    # print('update', dict_cat_depth['catalog_srl'], dict_cat_depth['cat_title'], 'to', s_val)
                                    self.__g_oSvDb.executeQuery('updateCatalogDepthBySrl', s_val, dict_cat_depth['catalog_srl'])
                                    b_changed_something = True
                                b_proc = True
                    if not b_proc:  # add new cat depth info
                        # print('add new', s_cat_depth, s_val)
                        self.__g_oSvDb.executeQuery('insertCatalogDepth', n_item_srl, s_cat_depth, s_val)
                        b_changed_something = True
                else:  # remove existed depth
                    s_cat_depth = s_key.replace('s_cat', '')
                    if lst_cat_depth is not None: 
                         for dict_cat_depth in lst_cat_depth:
                             if s_cat_depth == str(dict_cat_depth['cat_depth']):
                                if dict_cat_depth['cat_title'] != s_val:
                                    # print('remove', dict_cat_depth['catalog_srl'], dict_cat_depth['cat_title'])
                                    self.__g_oSvDb.executeQuery('deleteCatalogDepthBySrl', dict_cat_depth['catalog_srl'])
                                    b_changed_something = True
                                b_proc = True
        
        # begin - proc ignore item
        lst_item_srl_tobe_ignored = request.POST.getlist('item_hide[]')
        lst_ignored_item = []
        lst_ignored_item_rst = self.__g_oSvDb.executeQuery('getGaItemListIgnored')
        for dict_item_ignored in lst_ignored_item_rst:
            lst_ignored_item.append(str(dict_item_ignored['item_srl']))
        del lst_ignored_item_rst

        # item to change to hide
        lst_change_hide = list(set(lst_item_srl_tobe_ignored) - set(lst_ignored_item))
        for s_item_srl in lst_change_hide:
            self.__g_oSvDb.executeQuery('updateGaItemListIgnored', 1, s_item_srl)
            b_changed_something = True

        # item to change to display
        lst_change_display = list(set(lst_ignored_item) - set(lst_item_srl_tobe_ignored))
        for s_item_srl in lst_change_display:
            self.__g_oSvDb.executeQuery('updateGaItemListIgnored', 0, s_item_srl)
            b_changed_something = True
        del lst_change_hide
        del lst_change_display
        # end - proc ignore item

        # begin - clear old denormed item perf table on BI DB
        if b_changed_something:
            s_plugin_name = 'client_serve'
            o_job_plugin = importlib.import_module('svplugins.' + s_plugin_name + '.task')
            lst_command = [s_plugin_name, 'mode=clear_ga_itemperf_sql', 'config_loc='+str(n_sv_acct_id) + '/' + str(n_brand_id)]
            with o_job_plugin.svJobPlugin() as o_job: # to enforce each plugin follow strict guideline or remove from scheduler
                o_job.set_my_name(s_plugin_name)
                o_job.parse_command(lst_command)
                o_job.do_task(None)
        # end - clear old denormed item perf table on BI DB            
        return

    def __get_cat_depth_dictionary(self):
        """ 
        construct cat depth dictionary 
        this method should be streamlined with svplugins.client_serve.ga_itemperf_log.__get_cat_depth_dictionary()
        """
        dict_arranged_catalog_depth = {}
        lst_cat_depth_rst = self.__g_oSvDb.executeQuery('getGaItemDepthAll')
        for dict_single_cat in lst_cat_depth_rst:
            n_item_srl = dict_single_cat['item_srl']
            # n_cat_depth = dict_single_cat['cat_depth']
            if n_item_srl not in dict_arranged_catalog_depth:
                dict_arranged_catalog_depth[n_item_srl] = []
            dict_arranged_catalog_depth[n_item_srl].append(dict_single_cat)
        del lst_cat_depth_rst
        return dict_arranged_catalog_depth
