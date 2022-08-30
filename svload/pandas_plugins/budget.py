from dateutil.relativedelta import relativedelta
from django.utils.html import strip_tags
from datetime import datetime
from datetime import date
import pandas as pd

# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


# 매출 목표는 ERP 정보를 사람이 직접 참고하는 것이 더 현실적임
class Budget:
    # __g_bPeriodDebugMode = False
    __g_oSvDb = None
    # .svcommon.sv_campaign_parser 와 병합 검토
    __g_dictBudgetType = {1: {'title': 'GOOGLE_ADS', 'media_rst_type': 'PS', 'media_source': 'google',
                              'media_media': 'cpc', 'desc': 'GDN, 구글 키워드 광고 예산', 'camp_prefix': 'GG_PS_CPC_'},
                          2: {'title': 'YOUTUBE', 'media_rst_type': 'PS', 'media_source': 'youtube',
                              'media_media': 'display', 'desc': '유튜브 동영상 광고 예산', 'camp_prefix': 'YT_PS_DISP_'},
                          3: {'title': 'FACEBOOK', 'media_rst_type': 'PS', 'media_source': 'facebook',
                              'media_media': 'cpc', 'desc': '페이스북 광고 예산', 'camp_prefix': 'FB_PS_CPC_'},
                          4: {'title': 'NVR_CPC', 'media_rst_type': 'PS', 'media_source': 'naver',
                              'media_media': 'cpc', 'desc': '네이버 키워드 광고 예산', 'camp_prefix': 'GG_PS_CPC_'},
                          5: {'title': 'NVR_SEO', 'media_rst_type': 'PNS', 'media_source': 'naver',
                              'media_media': 'organic', 'desc': '네이버 블로그 바이럴 예산', 'camp_prefix': 'NV_PNS_REF_'},
                          6: {'title': 'NVR_BRS', 'media_rst_type': 'PS', 'media_source': 'naver',
                              'media_media': 'display', 'desc': '네이버 브랜드 검색 페이지 예산', 'camp_prefix': 'NV_PS_DISP_'},
                          7: {'title': 'KAKAO_CPC', 'media_rst_type': 'PS', 'media_source': 'kakao',
                              'media_media': 'cpc', 'desc': '카카오 모먼트 예산', 'camp_prefix': 'KKO_PS_CPC_'},
                          100: {'title': 'ETC', 'media_rst_type': None, 'media_source': None,
                                'media_media': None, 'desc': '기타 비용', 'camp_prefix': None}
                          }

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

    def get_budget_type_dict(self):
        return self.__g_dictBudgetType

    def get_budget_amnt_by_period(self, dt_start, dt_end):
        """
        gross budget amnt for GA & Media dashboard
        :param dt_start: dt_req_first_day
        :param dt_end: dt_req_last_day
        :return:
        """
        # begin - construct budget list
        dict_budget = {}
        lst_budget_forward_rst = self.__g_oSvDb.executeQuery('getBudgetAmntByStartMonth', dt_start.year, dt_start.month)
        if lst_budget_forward_rst and 'err_code' in lst_budget_forward_rst[0].keys():  # for an initial stage; no table
            lst_budget_forward_rst = []
        # print(lst_budget_forward_rst)
        for dict_row in lst_budget_forward_rst:
            dict_budget[dict_row['id']] = dict_row
        del lst_budget_forward_rst

        lst_budget_backward_rst = self.__g_oSvDb.executeQuery('getBudgetAmntByEndMonth', dt_end.year, dt_end.month)
        if lst_budget_backward_rst and 'err_code' in lst_budget_backward_rst[0].keys():  # for an initial stage; no table
            lst_budget_backward_rst = []
        for dict_row in lst_budget_backward_rst:
            dict_budget[dict_row['id']] = dict_row
        del lst_budget_backward_rst
        # end - construct budget list

        # begin - calculate period allocated budget list
        dict_budget_to_display = {}
        for n_budget_id, dict_single_budget in dict_budget.items():
            dt_start_tmp = dt_start.date()  # first day of the month
            dt_end_tmp = dt_end.date()  # last day of the month
            dt_delta = dict_single_budget['date_end'] - dict_single_budget['date_begin']
            n_full_budget_days = dt_delta.days + 1
            del dt_delta

            dt_period_start = dict_single_budget['date_begin']
            dt_period_end = dict_single_budget['date_end']
            if dict_single_budget['date_begin'] < dt_start_tmp:
                dt_period_start = dt_start_tmp
            if dict_single_budget['date_end'] > dt_end_tmp:
                dt_period_end = dt_end_tmp
            del dt_start_tmp, dt_end_tmp

            dt_delta = dt_period_end - dt_period_start
            n_alloc_budget_days = dt_delta.days + 1
            n_target_amnt_inc_vat_alloc = int(dict_single_budget['target_amnt_inc_vat'] * n_alloc_budget_days / n_full_budget_days)

            n_acct_id = dict_single_budget['acct_id']
            dict_acct_info = self.__g_dictBudgetType[n_acct_id]
            if dict_acct_info['media_rst_type'] is None:
                continue
            s_source_medium = dict_acct_info['media_source'] + '_' + dict_acct_info['media_media']

            if s_source_medium in dict_budget_to_display.keys():  # update existing
                dict_budget_to_display[s_source_medium]['n_budget_tgt_amnt_inc_vat'] = \
                    dict_budget_to_display[s_source_medium]['n_budget_tgt_amnt_inc_vat'] + n_target_amnt_inc_vat_alloc
                if dict_budget_to_display[s_source_medium]['dt_period_start'] > dt_period_start:
                    dict_budget_to_display[s_source_medium]['dt_period_start'] = dt_period_start
                if dict_budget_to_display[s_source_medium]['dt_period_end'] < dt_period_end:
                    dict_budget_to_display[s_source_medium]['dt_period_end'] = dt_period_end
            else:  # add new
                dict_budget_to_display[s_source_medium] = {'s_source': dict_acct_info['media_source'],
                                                           's_media': dict_acct_info['media_media'],
                                                           'dt_period_start': dt_period_start,
                                                           'dt_period_end': dt_period_end,
                                                           'n_budget_tgt_amnt_inc_vat': n_target_amnt_inc_vat_alloc,
                                                           'b_campaign_level': False}
            if dict_single_budget['memo'].startswith(dict_acct_info['camp_prefix']):
                dict_budget_to_display[s_source_medium]['b_campaign_level'] = True
                if 'dict_campaign' not in dict_budget_to_display[s_source_medium].keys():
                    dict_budget_to_display[s_source_medium]['dict_campaign'] = {}  # append dict_campaign attr
                s_campaign_title = dict_single_budget['memo']
                if s_campaign_title not in dict_budget_to_display[s_source_medium]['dict_campaign'].keys():  # register new dict_campaign attr
                    dict_budget_to_display[s_source_medium]['dict_campaign'][s_campaign_title] = {}
                    dict_budget_to_display[s_source_medium]['dict_campaign'][s_campaign_title] = \
                        {'dt_period_start': dt_period_start, 'dt_period_end': dt_period_end,
                         'n_budget_tgt_amnt_inc_vat': n_target_amnt_inc_vat_alloc}
                else:  # add additional dict_campaign attr
                    if dt_period_start < dict_budget_to_display[s_source_medium]['dict_campaign'][s_campaign_title]['dt_period_start']:
                        dict_budget_to_display[s_source_medium]['dict_campaign'][s_campaign_title]['dt_period_start'] = dt_period_start
                    if dt_period_end > dict_budget_to_display[s_source_medium]['dict_campaign'][s_campaign_title]['dt_period_end']:
                        dict_budget_to_display[s_source_medium]['dict_campaign'][s_campaign_title]['dt_period_end'] = dt_period_end
                    dict_budget_to_display[s_source_medium]['dict_campaign'][s_campaign_title]['n_budget_tgt_amnt_inc_vat'] += n_target_amnt_inc_vat_alloc
        # end - calculate period allocated budget list
        return dict_budget_to_display

    def get_detail_by_id(self, n_budget_id):
        """
        data for budget detail screen
        :param n_budget_id:
        :return:
        """
        lst_budget_detail = self.__g_oSvDb.executeQuery('getBudgetDetailByBudgetId', n_budget_id)
        lst_budget_detail[0]['s_budget_yrmo'] = '{0:04d}{1:02d}'.format(lst_budget_detail[0]['alloc_yr'], 
                                                                        lst_budget_detail[0]['alloc_mo'])
        del lst_budget_detail[0]['alloc_yr']
        del lst_budget_detail[0]['alloc_mo']
        return lst_budget_detail[0]

    def get_list_by_period(self, s_period_from, s_period_to):
        """
        data for budget list screen
        :param s_period_from:
        :param s_period_to:
        :return:
        """
        dt_today = date.today()
        lst_budget_earliest = self.__g_oSvDb.executeQuery('getBudgetEarliest')
        lst_budget_latest = self.__g_oSvDb.executeQuery('getBudgetLatest')
        if lst_budget_earliest[0]['min_date'] is None or lst_budget_latest[0]['max_date'] is None:
            dt_latest_budget = datetime.today()
            dt_earliest_budget = dt_latest_budget - relativedelta(months=1)
            dt_earliest_budget = dt_earliest_budget.replace(day=1)
        else:
            dt_earliest_budget = lst_budget_earliest[0]['min_date']
            dt_latest_budget = lst_budget_latest[0]['max_date']
        del lst_budget_earliest, lst_budget_latest

        if s_period_from is not None and s_period_to is not None:
            dt_earliest_req = datetime.strptime(s_period_from, '%Y%m%d')
            dt_latest_req = datetime.strptime(s_period_to, '%Y%m%d')
        else:
            dt_latest_req = dt_latest_budget
            dt_earliest_req = dt_latest_req - relativedelta(months=3)
            dt_earliest_req = dt_earliest_req.replace(day=1)
        # TODO Fix bug: display completed expenditure on a planning budget acct 미례 예산에 과거 비용이 표시되는 문제
        # begin - build period slot for monthly budget progress graph
        idx_monthly_budget_period = pd.date_range(start=dt_earliest_req, end=dt_latest_req, freq='M')
        dict_budget_monthly = {}
        for budget_month in idx_monthly_budget_period:
            dict_budget_monthly[budget_month.strftime("%Y%m")] = {'tgt_budget_amnt_inc_vat': 0, 'act_spent_amnt_inc_vat': 0 }
        del idx_monthly_budget_period
        # end - build period slot for monthly budget progress graph
        lst_rst = self.__g_oSvDb.executeQuery('getBudgetDetailByPeriod', dt_earliest_req, dt_latest_req)
        lst_added_rst = []
        n_tgt_budget_inc_vat = 0
        n_act_spent_inc_vat = 0
        for dict_budget in lst_rst:
            b_gross_cost_inc_vat_proc = False
            n_gross_cost_inc_vat = 0
            n_acct_id = dict_budget['acct_id']
            dict_acct_info = self.__g_dictBudgetType[n_acct_id]
            if dict_acct_info['title'] == 'NVR_BRS' or dict_acct_info['title'] == 'ETC':
                if dict_budget['date_begin'] <= dt_today:  # if budget begin date is past than today
                    n_gross_cost_inc_vat = dict_budget['target_amnt_inc_vat']
                    b_gross_cost_inc_vat_proc = True
            elif dict_acct_info['title'] == 'NVR_SEO':
                if dict_budget['date_begin'] > dt_today:  # if budget begin date is future than today
                    b_gross_cost_inc_vat_proc = True
                elif dict_budget['date_begin'] <= dt_today and dict_budget['date_end'] >= dt_today:  # if budget period is between today
                    n_gross_cost_inc_vat = self.__get_gross_cost_inc_vat(dict_acct_info, dict_budget['date_begin'])    
                    b_gross_cost_inc_vat_proc = True
                elif dict_budget['date_end'] < dt_today:  # if budget end date is past than today
                    n_gross_cost_inc_vat = dict_budget['target_amnt_inc_vat']
                    b_gross_cost_inc_vat_proc = True

            if not b_gross_cost_inc_vat_proc:
                n_gross_cost_inc_vat = self.__get_gross_cost_inc_vat(dict_acct_info, dict_budget['date_begin'])
            
            s_alloc_yr_mo = str(dict_budget['alloc_yr']) + '{:02d}'.format(dict_budget['alloc_mo'])
            # begin - build account level table info
            dict_tmp = {'id': dict_budget['id'], 'title': dict_acct_info['title'],
                        'desc': dict_acct_info['desc'],
                        'alloc_period': s_alloc_yr_mo,
                        'memo': dict_budget['memo'],
                        'target_amnt_inc_vat': dict_budget['target_amnt_inc_vat'],
                        'actual_amnt_inc_vat': n_gross_cost_inc_vat,
                        'date_begin': dict_budget['date_begin'], 'date_end': dict_budget['date_end'],
                        'regdate': dict_budget['regdate']}
            lst_added_rst.append(dict_tmp)
            # end - build account level table info
            # begin - build monthly budget bar-graph info
            try:
                dict_budget_monthly[s_alloc_yr_mo]['tgt_budget_amnt_inc_vat'] = dict_budget_monthly[s_alloc_yr_mo]['tgt_budget_amnt_inc_vat'] + int(dict_budget['target_amnt_inc_vat'])
                dict_budget_monthly[s_alloc_yr_mo]['act_spent_amnt_inc_vat'] = dict_budget_monthly[s_alloc_yr_mo]['act_spent_amnt_inc_vat'] + int(n_gross_cost_inc_vat)
            except KeyError:  # regards exceptional future budget
                dict_budget_monthly[s_alloc_yr_mo] = {'tgt_budget_amnt_inc_vat': int(dict_budget['target_amnt_inc_vat']),
                                                      'act_spent_amnt_inc_vat': int(n_gross_cost_inc_vat)}
            n_tgt_budget_inc_vat = n_tgt_budget_inc_vat + int(dict_budget['target_amnt_inc_vat'])
            n_act_spent_inc_vat = n_act_spent_inc_vat + int(n_gross_cost_inc_vat)
            # end - build monthly budget bar-graph info
        del lst_rst
        # begin - reorganize index to draw budget progress graph
        lst_budget_monthly_period = []
        lst_budget_monthly_tgt = []
        lst_spent_monthly_act = []
        # for s_alloc_yr_mo, dict_row in dict_budget_monthly.items():
        # sort dict_budget_monthly by year month tag
        lst_budget_yr_mo = sorted(list(dict_budget_monthly.keys()))
        for s_alloc_yr_mo in lst_budget_yr_mo:
            lst_budget_monthly_period.append(s_alloc_yr_mo)
            dict_row = dict_budget_monthly[s_alloc_yr_mo]
            lst_budget_monthly_tgt.append(dict_row['tgt_budget_amnt_inc_vat'])
            lst_spent_monthly_act.append(dict_row['act_spent_amnt_inc_vat'])
        del lst_budget_yr_mo
        del dict_budget_monthly
        n_tgt_act_gap_inc_vat = abs(n_tgt_budget_inc_vat - n_act_spent_inc_vat)
        if n_tgt_act_gap_inc_vat > 0:
            s_tgt_act_gap_status = '여유'
        else:
            s_tgt_act_gap_status = '초과'
        dict_budget_progress = {'lst_budget_monthly_period': lst_budget_monthly_period,
                                'lst_budget_monthly_tgt': lst_budget_monthly_tgt,
                                'lst_spent_monthly_act': lst_spent_monthly_act,
                                'n_tgt_budget_inc_vat': n_tgt_budget_inc_vat,
                                'n_act_spent_inc_vat': n_act_spent_inc_vat,
                                'n_tgt_act_gap_inc_vat': n_tgt_act_gap_inc_vat,
                                's_tgt_act_gap_status': s_tgt_act_gap_status,
                                }
        # end - reorganize index to draw budget progress graph
        dict_budget_period = {'s_earliest_budget': dt_earliest_budget.strftime("%Y%m%d"),
                              's_latest_budget': dt_latest_budget.strftime("%Y%m%d"),
                              's_earliest_req': dt_earliest_req.strftime("%Y%m%d"),
                              's_latest_req': dt_latest_req.strftime("%Y%m%d")}
        del dt_earliest_budget, dt_latest_budget
        return {'dict_budget_period': dict_budget_period, 'lst_added_rst': lst_added_rst,
                'dict_budget_progress': dict_budget_progress }

    def get_acct_list_for_ui(self):
        """
        elements for appending single budget on the budget list screen
        :return:
        """
        lst_acct_info = []
        for n_acct_id, dict_acct in self.__g_dictBudgetType.items():
            lst_acct_info.append({'n_acct_id': n_acct_id, 's_budget_acct_ttl': dict_acct['title'],
                                  'desc': dict_acct['desc']})
        return lst_acct_info

    def update_budget(self, n_budget_id, request):
        dict_rst = self.__validate_budget_info(request)
        if not dict_rst['b_error']:
            dict_budget = dict_rst['dict_ret']
            self.__g_oSvDb.executeQuery('updateBudgetByBudgetId', dict_budget['n_acct_id'],
                                        dict_budget['dt_budget_alloc_yr_mo'].year, dict_budget['dt_budget_alloc_yr_mo'].month,
                                        dict_budget['s_budget_memo'], dict_budget['n_budget_target_amnt_inc_vat'],
                                        dict_budget['dt_budget_date_begin'], dict_budget['dt_budget_date_end'], n_budget_id)
            del dict_budget
        return dict_rst

    def add_budget(self, request):
        """
        add budget
        :param request:
        :return:
        """
        dict_rst = self.__validate_budget_info(request)
        if not dict_rst['b_error']:
            dict_budget = dict_rst['dict_ret']
            self.__g_oSvDb.executeQuery('insertBudget', request.user.pk, dict_budget['n_acct_id'],
                                        dict_budget['dt_budget_alloc_yr_mo'].year, dict_budget['dt_budget_alloc_yr_mo'].month,
                                        dict_budget['s_budget_memo'], dict_budget['n_budget_target_amnt_inc_vat'],
                                        dict_budget['dt_budget_date_begin'], dict_budget['dt_budget_date_end'])
            del dict_budget
        return dict_rst

    def __validate_budget_info(self, request):
        dict_rst = {'b_error': True, 's_msg': None, 'dict_ret': None}
        # begin - validation
        n_acct_id = int(request.POST['acct_id'])
        if n_acct_id not in list(self.__g_dictBudgetType.keys()):
            dict_rst['s_msg'] = 'invalid acct id'
            return dict_rst

        s_budget_alloc_period = strip_tags(request.POST['budget_alloc_period'].strip())
        s_budget_memo = strip_tags(request.POST['budget_memo'].strip())  # <script>console.log('dd')</script> 방지해야 함
        s_budget_target_amnt_inc_vat = request.POST['budget_target_amnt_inc_vat'].strip()
        s_budget_date_begin = request.POST['budget_date_begin'].strip()
        s_budget_date_end = request.POST['budget_date_end'].strip()

        try:
            dt_budget_alloc_yr_mo = datetime.strptime(s_budget_alloc_period, '%Y%m')
        except ValueError:
            dict_rst['s_msg'] = 'invalid budget yr mo'
            return dict_rst

        if len(s_budget_memo) == 0:
            dict_rst['s_msg'] = 'blank budget memo'
            return dict_rst

        s_budget_target_amnt_inc_vat = s_budget_target_amnt_inc_vat.replace(',', '').strip()
        try:
            n_budget_target_amnt_inc_vat = int(s_budget_target_amnt_inc_vat)
        except ValueError:
            dict_rst['s_msg'] = 'invalid target_amnt_inc_vat'
            return dict_rst

        try:
            dt_budget_date_begin = datetime.strptime(s_budget_date_begin, '%Y-%m-%d')
        except ValueError:
            dict_rst['s_msg'] = 'invalid budget_date_begin'
            return dict_rst

        try:
            dt_budget_date_end = datetime.strptime(s_budget_date_end, '%Y-%m-%d')
        except ValueError:
            dict_rst['s_msg'] = 'invalid budget_date_end'
            return dict_rst

        dict_rst['b_error'] = False
        dict_rst['dict_ret'] = {'n_acct_id': n_acct_id, 'dt_budget_alloc_yr_mo': dt_budget_alloc_yr_mo,
                                's_budget_memo': s_budget_memo, 'n_budget_target_amnt_inc_vat': n_budget_target_amnt_inc_vat,
                                'dt_budget_date_begin': dt_budget_date_begin, 'dt_budget_date_end': dt_budget_date_end}
        return dict_rst

    def __get_gross_cost_inc_vat(self, dict_acct_info, dt_budget_date_begin):
        dt_first_day_budget_month = dt_budget_date_begin.replace(day=1)
        dt_last_day_budget_month = self.__get_last_day_of_month(dt_first_day_budget_month)

        lst_cost_rst = self.__g_oSvDb.executeQuery('getMediaExpense',
                                                   dict_acct_info['media_rst_type'],
                                                   dict_acct_info['media_source'],
                                                   dict_acct_info['media_media'],
                                                   dt_first_day_budget_month,
                                                   dt_last_day_budget_month)
        n_gross_cost_inc_vat = 0
        for s_title, n_value in lst_cost_rst[0].items():
            if n_value is not None:
                n_gross_cost_inc_vat = n_gross_cost_inc_vat + n_value
        return n_gross_cost_inc_vat

    def __get_last_day_of_month(self, dt_source):
        if dt_source.month == 12:
            return dt_source.replace(day=31)
        return dt_source.replace(month=dt_source.month + 1, day=1) - relativedelta(days=1)
