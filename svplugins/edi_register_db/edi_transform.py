# -*- coding: UTF-8 -*-
# UTF-8 테스트

# Copyright 2021 singleview.co.kr, Inc.

# You are hereby granted a non-exclusive, worldwide, royalty-free license to
# use, copy, modify, and distribute this software in source code or binary
# form for use in connection with the web services and APIs provided by
# singleview.co.kr.

# As with any software that integrates with the singleview.co.kr platform, 
# your use of this software is subject to the Facebook Developer Principles 
# and Policies [http://singleview.co.kr/api_policy/]. This copyright 
# notice shall be included in all copies or substantial portions of the 
# software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

# standard library

# singleview library
if __name__ == 'edi_transform': # for console debugging
    import sv_hypermart_model
else: # for platform running
    from svcommon import sv_hypermart_model


class TransformEdiDb:
    __g_dictBranchInfoById = {}
    __g_nLimitToSingleQuery = 100000  # prevent memory dump, when loads big data

    def __new__(cls):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)  # turn to decorator
        return super().__new__(cls)

    def __init__(self):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        self.__g_oSvDb = None
        self.__g_dictSkuInfoById = {}
        self.__g_bDebugMode = False

        self.__continue_iteration = None
        self.__print_debug = None
        self.__print_progress_bar = None
        super().__init__()

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __del__(self):
        self.__continue_iteration = None
        self.__print_debug = None
        self.__print_progress_bar = None
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        return self

    def init_var(self, f_print_debug, f_print_progress_bar, f_continue_iteration):
        self.__continue_iteration = f_continue_iteration
        self.__print_debug = f_print_debug
        self.__print_progress_bar = f_print_progress_bar

    def initialize(self, o_sv_mysql, dict_param):
        self.__g_oSvDb = o_sv_mysql
        if not self.__g_oSvDb:
            self.__print_debug('invalid db handler')
            # raise Exception('invalid db handler')

        b_excel_pivot = False
        b_aws_quicksight = False
        b_oracle_analytics_cloud = False

        if 'b_debug_mode' in dict_param:
            if dict_param['b_debug_mode']:
                self.__g_bDebugMode = True
        if 'b_excel_pivot' in dict_param:
            if dict_param['b_excel_pivot']:
                b_excel_pivot = True
        if 'b_aws_quicksight' in dict_param:
            if dict_param['b_aws_quicksight']:
                b_aws_quicksight = True
        if 'b_oracle_analytics_cloud' in dict_param:
            if dict_param['b_oracle_analytics_cloud']:
                b_oracle_analytics_cloud = True

        # self.__g_oSvDb = SvSqlAccess()
        # if not self.__g_oSvDb:
        #     raise Exception('invalid db handler')
        # self.__g_oSvDb.set_tbl_prefix(dict_param['s_tbl_prefix'])
        # self.__g_oSvDb.set_app_name(__name__)
        # self.__g_oSvDb.initialize()
        # self.__g_oSvDb.set_reserved_tag_value({'brand_id': dict_param['n_brand_id']})

        # self.__g_sExtractFilenamePrefix = dict_param['s_extract_filename_prefix']
        self.__print_debug('start extract_db()')
        self.__extract_branch_and_skus(dict_param)
        if b_excel_pivot:
            self.__extract_excel_pivot(dict_param)
        if b_oracle_analytics_cloud:
            self.__extract_for_oac_desktop(dict_param)
        if b_aws_quicksight:
            self.__extract_for_aws_quicksight(dict_param)
        self.__print_debug('finish extract_db()')
        # return 'Done!'

    def __extract_branch_and_skus(self, dict_param):
        """
        retrieve branches and skus info
        :param dict_param:
        :return:
        """
        dict_branch_by_title = sv_hypermart_model.SvHyperMartType.get_dict_by_title()
        dict_branch_type = sv_hypermart_model.BranchType.get_dict_by_title()
        o_mart_geo_info = sv_hypermart_model.SvHypermartGeoInfo()

        lst_hypermart_geo_info = o_mart_geo_info.lst_hypermart_geo_info
        if len(lst_hypermart_geo_info):
            # for single_branch in o_branch_info:
            # for dict_single_branch in lst_hypermart_geo_info:
            #     self.__g_dictBranchInfoById[dict_single_branch['id']] = {
            #         'mart': single_branch.get_hypermart_type_label(),  # emart, lottemart
            #         'type': single_branch.get_branch_type_label(),  # on & off
            #         'name': single_branch.branch_name,
            #         'do': single_branch.do_name,
            #         'si': single_branch.si_name,
            #         'gu': single_branch.gu_gun,
            #         'dong': single_branch.dong_myun_ri,
            #         'longi': single_branch.longitude,
            #         'lati': single_branch.latitude
            #     }
            for dict_single_branch in o_mart_geo_info.lst_hypermart_geo_info:
                n_hypermart_id = dict_branch_by_title[dict_single_branch['hypermart_name']]
                n_branch_type_id = dict_branch_type[dict_single_branch['branch_type']]
                dict_branch = {'mart': dict_single_branch['hypermart_name'],  # emart, lottemart
                               'type': dict_single_branch['branch_type'],  # on & off
                               'name': dict_single_branch['name'],
                               'do': dict_single_branch['do_name'],
                               'si': dict_single_branch['si_name'],
                               'gu': dict_single_branch['gu_gun'],
                               'dong': dict_single_branch['dong_myun_ri'],
                               'longi': dict_single_branch['longitude'],
                               'lati': dict_single_branch['latitude']}
                self.__g_dictBranchInfoById[dict_single_branch['id']] = dict_branch

            # write emart branch info csv
            # s_full_path_extracted_csv = os.path.join(settings.MEDIA_ROOT, settings.EDI_STORAGE_ROOT,
            #                                          str(dict_param['n_owner_id']), 'edi_branches.csv')
            # try:
            #     with codecs.open(s_full_path_extracted_csv, "w", "utf-8") as f:
            #         writer = csv.writer(f)
            #         # write csv header
            #         writer.writerow(['branch_id', 'branch_name', 'branch_type', 'do_name', 'si_name', 'gu_gun',
            #                          'dong_myun_ri', 'longitude', 'latitude'])
            #         # write csv body
            #         for single_branch in o_branch_info:
            #             writer.writerow([single_branch.id,
            #                              single_branch.branch_name,
            #                              single_branch.get_branch_type_label(),
            #                              single_branch.do_name,
            #                              single_branch.si_name,
            #                              single_branch.gu_gun,
            #                              single_branch.dong_myun_ri,
            #                              single_branch.longitude,
            #                              single_branch.latitude])
            # except Exception as e:
            #     del o_branch_info
            #     print(e)
        else:
            del dict_branch_by_title
            del dict_branch_type
            del o_mart_geo_info
            print('excel extraction failure - no branch info')
            return
        
        del dict_branch_by_title
        del dict_branch_type
        del o_mart_geo_info

        # write selected sku info csv
        # retrieve account specific SKU info dictionary from account dependent table
        
        # for dictSingleRow in lst_rst:
        #     self.__g_dictSkuInfoById[str(dictSingleRow['mart_id']) + '_' + dictSingleRow['item_code']] = dictSingleRow['id']
        # return

        # lst_sku_info_rst = self.__g_oSvDb.execute_query('getEdiSkuByBrandId', dict_param['n_brand_id'])
        lst_sku_info_rst = self.__g_oSvDb.execute_query('getEdiSkuAccepted', 1)
        if len(lst_sku_info_rst):
            for dict_single_sku in lst_sku_info_rst:
                self.__g_dictSkuInfoById[dict_single_sku['id']] = {
                    'name': dict_single_sku['item_name']
                }
            # s_full_path_extracted_csv = os.path.join(settings.MEDIA_ROOT, settings.EDI_STORAGE_ROOT,
            #                                          str(dict_param['n_owner_id']),
            #                                          self.__g_sExtractFilenamePrefix + '_edi_skus.csv')
            # try:
            #     with codecs.open(s_full_path_extracted_csv, "w", "utf-8") as f:
            #         writer = csv.writer(f)
            #         # write csv header
            #         writer.writerow(['item_id', 'item_code', 'item_name', 'first_detect_date'])
            #         # write csv body
            #         for dict_single_sku in lst_sku_info_rst:
            #             writer.writerow([dict_single_sku['id'],
            #                              dict_single_sku['item_code'],
            #                              dict_single_sku['item_name'],
            #                              dict_single_sku['first_detect_logdate']])
            # except Exception as e:
            #     print(e)
        del lst_sku_info_rst

    def __extract_excel_pivot(self, dict_param):
        """
        retrieve specific period for excel pivoting
        or
        retrieve specific period for Google Data Studio
        :param dict_param:
        :return:
        """
        self.__print_debug('start excel extraction')
        self.__g_oSvDb.create_table_on_demand('_edi_daily_log_denorm')
        # self.__g_oSvDb.truncate_table('_edi_daily_log_denorm')
        if not dict_param['s_period_start'] or not dict_param['s_period_end']:
            self.__print_debug('excel extraction failure - no period selected')
            return
        dict_param_tmp = {'s_period_start': dict_param['s_period_start'],
                          's_period_end': dict_param['s_period_end']}

        # s_full_path_extracted_csv = os.path.join(settings.MEDIA_ROOT, settings.EDI_STORAGE_ROOT,
        #                                          str(dict_param['n_owner_id']),
        #                                          self.__g_sExtractFilenamePrefix + '_excel_pivot.csv')
        # try:
        #     f = open(s_full_path_extracted_csv, 'w', encoding='euc-kr')
        # except Exception as e:
        #     print(e)
        #     return
        lst_mart = ['Emart', 'Ltmart']
        for s_mart_title in lst_mart:
            s_log_cnt_query = 'get{s_mart_title}LogCountByPeriod'.format(s_mart_title=s_mart_title)
            lst_log_count = self.__g_oSvDb.execute_query(s_log_cnt_query, dict_param['s_period_start'],
                                                         dict_param['s_period_end'])
            n_edi_log_count = lst_log_count[0]['count(*)']
            del lst_log_count

            # print(n_edi_log_count)

            s_performance_log_query = 'get{s_mart_title}LogByPeriod'.format(s_mart_title=s_mart_title)
            # begin - mart extraction by mart
            

            n_limit = self.__g_nLimitToSingleQuery
            n_offset = 0
            dict_param_tmp = {'s_period_start': dict_param['s_period_start'],
                              's_period_end': dict_param['s_period_end'],
                              'n_offset': n_offset, 'n_limit': n_limit}

            # writer = csv.writer(f)
            # # write csv header
            # lst_log_columns = ['branch_name', 'item_name', 'yr', 'mo', 'day', 'refund', 'qty', 'amnt']
            # if self.__g_bDebugMode:
            #     lst_log_columns = ['log_srl'] + lst_log_columns
            # writer.writerow(lst_log_columns)
            # del lst_log_columns
            # very big data causes memory dump, if retrieve at single access
            while n_edi_log_count > 0:
                dict_param_tmp['n_offset'] = n_offset
                dict_param_tmp['n_limit'] = n_limit
                n_offset = n_offset + n_limit
                n_edi_log_count = n_edi_log_count - n_limit
                if n_limit >= n_edi_log_count:
                    n_limit = n_edi_log_count

                lst_log_period = self.__g_oSvDb.execute_dynamic_query(s_performance_log_query, dict_param_tmp)
                # write csv body
                for dict_single_log in lst_log_period:
                    b_refund = 0
                    if int(dict_single_log['qty']) < 0:
                        b_refund = 1
                        n_amnt = dict_single_log['amnt'] * -1
                    else:
                        n_amnt = dict_single_log['amnt']
                    
                    # excel csv mode
                    # lst_final_data_cols = [self.__g_dictBranchInfoById[dict_single_log['branch_id']]['name'],
                    #                         self.__g_dictSkuInfoById[dict_single_log['item_id']]['name'],
                    #                         dict_single_log['logdate'].year,
                    #                         dict_single_log['logdate'].month,
                    #                         dict_single_log['logdate'].day,
                    #                         b_refund,
                    #                         dict_single_log['qty'],
                    #                         dict_single_log['amnt']]
                    # if self.__g_bDebugMode:
                    #     lst_final_data_cols = [dict_single_log['id']] + lst_final_data_cols
                    # writer.writerow(lst_final_data_cols)
                del lst_log_period
        # f.close()
        # end - extraction by mart
        # del dict_param_tmp
        # specific period for excel pivoting end
        self.__print_debug('excel extraction succeed')

    def __extract_for_oac_desktop(self, dict_param):
        """
        write EDI daily and GA-media daily log info csv for Oracle Analytics Cloud
        OAC allows multi independent tables in a single project
        OAC can integrate GA-media tbl and EDI tbl on logdate
        :param dict_param:
        :return:
        """
        self.__print_debug('start - OAC desktop extraction')
        dict_param_tmp = {'s_req_sku_set': dict_param['s_req_sku_set'], 'n_owner_id': dict_param['n_owner_id']}
        lst_log_count = self.__g_oSvDb.execute_dynamic_query('getEmartLogCountByItemId', dict_param_tmp)
        n_lst_log_count = lst_log_count[0]['count(*)']
        del lst_log_count, dict_param_tmp

        n_limit = self.__g_nLimitToSingleQuery
        n_offset = 0
        dict_param_tmp = {'s_req_sku_set': dict_param['s_req_sku_set'], 'n_owner_id': dict_param['n_owner_id'],
                          'n_offset': n_offset, 'n_limit': n_limit}
        s_full_path_extracted_csv = os.path.join(settings.MEDIA_ROOT, settings.EDI_STORAGE_ROOT,
                                                 str(dict_param['n_owner_id']),
                                                 self.__g_sExtractFilenamePrefix + '_edi_daily_log_oac.csv')
        try:
            with open(s_full_path_extracted_csv, "w", newline="") as f:
                writer = csv.writer(f)
                # write csv header
                lst_log_columns = ['item_id', 'branch_id', 'qty', 'amnt', 'log_yr', 'log_mo', 'log_day', 'log_qr',
                                   'log_yr_qr', 'log_wk_no', 'log_yr_wk_no', 'log_day_name', 'log_day_mo', 'log_day_yr',
                                   'logdate']
                if self.__g_bDebugMode:
                    lst_log_columns = ['log_id'] + lst_log_columns
                writer.writerow(lst_log_columns)
                del lst_log_columns

                # very big data causes memory dump, if retrieve at single access
                while n_lst_log_count > 0:
                    dict_param_tmp['n_offset'] = n_offset
                    dict_param_tmp['n_limit'] = n_limit
                    n_offset = n_offset + n_limit
                    n_lst_log_count = n_lst_log_count - n_limit
                    if n_limit >= n_lst_log_count:
                        n_limit = n_lst_log_count

                    # print(dict_param_tmp['n_offset'])
                    lst_rst = self.__g_oSvDb.execute_dynamic_query('getEmartLogByItemId', dict_param_tmp)
                    # https://strftime.org/
                    # write csv body
                    for dictSingleRow in lst_rst:
                        s_quarter = self.__get_quarter(dictSingleRow['logdate'])
                        s_yr = dictSingleRow['logdate'].strftime("%Y")
                        s_wk = dictSingleRow['logdate'].strftime("%V")
                        lst_final_data_cols = [dictSingleRow['item_id'], dictSingleRow['branch_id'],
                                               dictSingleRow['qty'], dictSingleRow['amnt'], s_yr,
                                               dictSingleRow['logdate'].strftime("%m"),
                                               dictSingleRow['logdate'].strftime("%d"),
                                               s_quarter, s_yr + '-' + s_quarter, s_wk, s_yr + '-' + s_wk,
                                               dictSingleRow['logdate'].strftime("%a"),
                                               dictSingleRow['logdate'].strftime("%d"),
                                               dictSingleRow['logdate'].strftime("%j"),
                                               str(dictSingleRow['logdate']).replace('-', '')]
                        if self.__g_bDebugMode:
                            lst_final_data_cols = [dictSingleRow['id']] + lst_final_data_cols
                        writer.writerow(lst_final_data_cols)
                        del lst_final_data_cols
                    del lst_rst
        except Exception as e:
            self.__print_debug(e)

        # write gross GA-media daily log info csv
        dict_tag = {'brand_id': dict_param['n_brand_id']}
        self.__g_oSvDb.set_reserved_tag_value(dict_tag)
        # lst_rst = self.__g_oSvDb.execute_query('getGaMediaDailyLog')
        lst_rst_count = self.__g_oSvDb.execute_query('getGaMediaDailyLogCount')
        n_lst_log_count = lst_rst_count[0]['count(*)']
        del lst_rst_count

        n_limit = self.__g_nLimitToSingleQuery
        n_offset = 0
        dict_param_tmp = {'n_brand_id': dict_param['n_brand_id'], 'n_offset': n_offset, 'n_limit': n_limit}
        s_full_path_extracted_csv = os.path.join(settings.MEDIA_ROOT, settings.EDI_STORAGE_ROOT,
                                                 str(dict_param['n_owner_id']),
                                                 self.__g_sExtractFilenamePrefix + '_ga_media_daily_log_oac.csv')
        try:
            with codecs.open(s_full_path_extracted_csv, "w", "utf-8") as f:
                writer = csv.writer(f)
                # write csv header
                lst_log_columns = ['media_ua', 'media_term', 'media_source', 'media_rst_type', 'media_media',
                                   'media_brd', 'media_camp1st', 'media_camp2nd', 'media_camp3rd', 'media_raw_cost',
                                   'media_agency_cost', 'media_cost_vat', 'media_imp', 'media_click',
                                   'media_conv_cnt', 'media_conv_amnt', 'in_site_tot_session', 'in_site_tot_new',
                                   'in_site_tot_bounce', 'in_site_tot_duration_sec', 'in_site_tot_pvs',
                                   # 'in_site_trs', 'in_site_revenue', 'in_site_registrations',
                                   'logdate']
                if self.__g_bDebugMode:
                    lst_log_columns = ['log_srl'] + lst_log_columns
                writer.writerow(lst_log_columns)
                del lst_log_columns

                # write csv body
                # very big data causes memory dump, if retrieve at single access
                while n_lst_log_count > 0:
                    dict_param_tmp['n_offset'] = n_offset
                    dict_param_tmp['n_limit'] = n_limit
                    n_offset = n_offset + n_limit
                    n_lst_log_count = n_lst_log_count - n_limit
                    if n_limit >= n_lst_log_count:
                        n_limit = n_lst_log_count

                    # print(dict_param_tmp['n_offset'])
                    lst_rst = self.__g_oSvDb.execute_dynamic_query('getGaMediaDailyLog', dict_param_tmp)

                    # |@| may occurs error on AWS quick sight
                    # AWS quick sight decides column as INT only by lookup first a few '00' and denies campaign string
                    for dictSingleRow in lst_rst:
                        lst_final_data_cols = [dictSingleRow['media_ua'],
                                               dictSingleRow['media_term'].replace('|@|sv', 'sv_none'),
                                               dictSingleRow['media_source'],
                                               dictSingleRow['media_rst_type'],
                                               dictSingleRow['media_media'],
                                               dictSingleRow['media_brd'],
                                               dictSingleRow['media_camp1st'].replace('|@|sv', 'sv_none'),
                                               # .replace('00', 'sv_none'),
                                               dictSingleRow['media_camp2nd'].replace('|@|sv', 'sv_none'),
                                               # .replace('00', 'sv_none'),
                                               dictSingleRow['media_camp3rd'].replace('|@|sv', 'sv_none'),
                                               # .replace('00', 'sv_none'),
                                               dictSingleRow['media_raw_cost'],
                                               dictSingleRow['media_agency_cost'],
                                               dictSingleRow['media_cost_vat'],
                                               dictSingleRow['media_imp'],
                                               dictSingleRow['media_click'],
                                               dictSingleRow['media_conv_cnt'],
                                               dictSingleRow['media_conv_amnt'],
                                               dictSingleRow['in_site_tot_session'],
                                               dictSingleRow['in_site_tot_new'],
                                               dictSingleRow['in_site_tot_bounce'],
                                               dictSingleRow['in_site_tot_duration_sec'],
                                               dictSingleRow['in_site_tot_pvs'],
                                               # dictSingleRow['in_site_trs'],
                                               # dictSingleRow['in_site_revenue'],
                                               # dictSingleRow['in_site_registrations'],
                                               # dictSingleRow['logdate']]
                                               str(dictSingleRow['logdate']).replace('-', '')]
                        if self.__g_bDebugMode:
                            lst_final_data_cols = [dictSingleRow['log_srl']] + lst_final_data_cols
                        writer.writerow(lst_final_data_cols)
                        del lst_final_data_cols
                    del lst_rst
        except Exception as e:
            self.__print_debug(e)
        self.__print_debug('finish - OAC extraction')

    def __extract_for_aws_quicksight(self, dict_param):
        """
        write merged EDI daily and GA-media daily log info csv for AWS quicksight
        AWS Quick Sight allows one single joined tables in a single project
        :param dict_param:
        :return:
        """
        self.__print_debug('start - QWS quicksight extraction')
        dict_tag = {'brand_id': dict_param['n_brand_id']}
        self.__g_oSvDb.set_reserved_tag_value(dict_tag)

        # write gross GA-media-EDI unioned daily log info csv
        lst_rst = self.__g_oSvDb.execute_query('getEdiGaUnionAll')
        # print(lst_rst)
        if len(lst_rst):
            s_full_path_extracted_csv = os.path.join(settings.MEDIA_ROOT, settings.EDI_STORAGE_ROOT,
                                                     str(dict_param['n_owner_id']),
                                                     self.__g_sExtractFilenamePrefix +
                                                     '_ga_media_edi_daily_log_aws_quicksight.csv')
            try:
                with codecs.open(s_full_path_extracted_csv, "w", "utf-8") as f:
                    writer = csv.writer(f)
                    # write csv header
                    lst_log_columns = [
                        'media_ua', 'media_term', 'media_source', 'media_rst_type', 'media_media', 'media_brd',
                        'media_camp1st', 'media_camp2nd', 'media_camp3rd', 'media_raw_cost', 'media_agency_cost',
                        'media_cost_vat', 'media_imp', 'media_click', 'media_conv_cnt', 'media_conv_amnt',
                        'in_site_tot_session', 'in_site_tot_new', 'in_site_tot_bounce', 'in_site_tot_duration_sec',
                        'in_site_tot_pvs',
                        'item_id', 'branch_id', 'qty', 'amnt', 'logdate'
                    ]
                    # if b_debug_mode:
                    #    lst_log_columns = ['log_srl'] + lst_log_columns
                    writer.writerow(lst_log_columns)
                    del lst_log_columns
                    # write csv body
                    # |@| may occurs error on AWS quick sight
                    # AWS quick sight decides column as INT only by lookup first a few '00' and denies campaign string
                    for dictSingleRow in lst_rst:
                        # print(dictSingleRow)
                        lst_final_data_cols = [dictSingleRow['media_ua'],
                                               dictSingleRow['media_term'].replace('|@|sv', 'sv_none'),
                                               dictSingleRow['media_source'],
                                               dictSingleRow['media_rst_type'],
                                               dictSingleRow['media_media'],
                                               dictSingleRow['media_brd'],
                                               dictSingleRow['media_camp1st'].replace('|@|sv', 'sv_none'),
                                               # .replace('00', 'sv_none'),
                                               dictSingleRow['media_camp2nd'].replace('|@|sv', 'sv_none'),
                                               # .replace('00', 'sv_none'),
                                               dictSingleRow['media_camp3rd'].replace('|@|sv', 'sv_none'),
                                               # .replace('00', 'sv_none'),
                                               dictSingleRow['media_raw_cost'],
                                               dictSingleRow['media_agency_cost'],
                                               dictSingleRow['media_cost_vat'],
                                               dictSingleRow['media_imp'],
                                               dictSingleRow['media_click'],
                                               dictSingleRow['media_conv_cnt'],
                                               dictSingleRow['media_conv_amnt'],
                                               dictSingleRow['in_site_tot_session'],
                                               dictSingleRow['in_site_tot_new'],
                                               dictSingleRow['in_site_tot_bounce'],
                                               dictSingleRow['in_site_tot_duration_sec'],
                                               dictSingleRow['in_site_tot_pvs'],
                                               # dictSingleRow['in_site_trs'],
                                               # dictSingleRow['in_site_revenue'],
                                               # dictSingleRow['in_site_registrations'],
                                               dictSingleRow['item_id'],
                                               dictSingleRow['branch_id'],
                                               dictSingleRow['qty'],
                                               dictSingleRow['amnt'],
                                               dictSingleRow['logdate']]
                        # if b_debug_mode:
                        #    lst_final_data_cols = [dictSingleRow['log_srl']] + lst_final_data_cols
                        writer.writerow(lst_final_data_cols)
                    del lst_final_data_cols
            except Exception as e:
                self.__print_debug(e)
        del lst_rst
        self.__print_debug('finish - QWS quicksight extraction')

    def clear(self):
        self.__g_oSvDb = None
        self.__g_sExtractFilenamePrefix = None
        self.__g_bDebugMode = False
        self.__g_dictBranchInfoById.clear()
        # self.__print_debug('ExtractDb::clear() called')
        return

    def __get_quarter(self, dt):
        return 'Q' + str(int(math.ceil(dt.month / 3.)))
