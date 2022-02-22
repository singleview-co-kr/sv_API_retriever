# from collections import defaultdict
from decimal import *
# import pandas as pd

# begin - bokeh related
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, Range1d, Legend, FactorRange, HoverTool, Span  # , LegendItem, Legend,
from bokeh.transform import factor_cmap
# from bokeh.embed import components
# from bokeh.palettes import Category20_19
# from bokeh.layouts import row
# end - bokeh related

# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class Visualizer:
    __g_bPeriodDebugMode = False
    __g_dictLabelOrientation = {'h': 0, 'i': 1.0471, 'v': 1.570}  # i: inclined
    # 0  # 라벨 수평
    # np.pi/4 = 0.7853981633974483  # 라벨을 반시계 45도 회전
    # np.pi/3 = 1.0471975511965976  # 라벨을 반시계 70도 회전
    # np.pi/2 = 1.5707963267948966  # 라벨을 반시계 90도 회전
    # np.pi/1 = 3.141592653589793  # 라벨을 반시계 180도 회전
    __g_nClassId = None
    __g_dictSeriesValue = {}
    __g_dictSeriesLabel = {}
    __g_dictSeriesColor = {}
    __g_lstSeriesXLabel = []
    __g_sGraphTitle = None
    __g_nGraphHeight = None

    def __new__(cls):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)  # turn to decorator
        return super().__new__(cls)

    def __init__(self):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        # print(id(self))
        # list.append() method does not differ from other class attribute of same title classes
        self.__g_nClassId = id(self)
        self.__g_dictSeriesValue[self.__g_nClassId] = []
        self.__g_dictSeriesLabel[self.__g_nClassId] = []
        self.__g_dictSeriesColor[self.__g_nClassId] = []
        super().__init__()

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __del__(self):
        # logger.debug('__del__')
        pass

    def activate_debug(self):
        self.__g_bPeriodDebugMode = True

    def set_title(self, s_graph_title):
        self.__g_sGraphTitle = s_graph_title

    def set_height(self, h_graph_title):
        self.__g_nGraphHeight = h_graph_title

    def set_x_labels(self, lst_x_lbl):
        self.__g_lstSeriesXLabel = lst_x_lbl

    def append_series(self, s_series_lbl, s_series_color, lst_series_val):
        self.__g_dictSeriesLabel[self.__g_nClassId].append(s_series_lbl)
        self.__g_dictSeriesColor[self.__g_nClassId].append(s_series_color)

        if type(lst_series_val) == list:
            self.__g_dictSeriesValue[self.__g_nClassId].append(lst_series_val)
        else:
            raise Exception('weird dataset')

    def draw_multi_line(self, s_xlabel_orientation='h'):
        """
        # build multi line figures; tooltip이 작동하지 않고 계열 라벨이 두줄로 표시되지 않으므로 4개 계열 이하만 사용
        :param s_xlabel_orientation:
        :return:
        """

        # bokeh multi line example
        # https://stackoverflow.com/questions/31520951/plotting-multiple-lines-with-bokeh-and-pandas
        # https://stackoverflow.com/questions/31419388/bokeh-how-to-add-legend-to-figure-created-by-multi-line-method
        # validate series info
        n_series_cnt = self.__validate_series_info()

        # get series values
        lst_series_data = self.__g_dictSeriesValue[self.__g_nClassId]
        lst_series_label = self.__g_dictSeriesLabel[self.__g_nClassId]
        lst_series_color = self.__g_dictSeriesColor[self.__g_nClassId]

        # lst_xs = [lst_x_lbl] * n_lst_series_color_cnt  # propagate x labels
        lst_xs = [self.__g_lstSeriesXLabel] * n_series_cnt  # propagate x labels

        # begin - get proper y axis end
        n_max_y_axis = 0
        for series in lst_series_data:
            n_max_y = max(series)
            if n_max_y > n_max_y_axis:
                n_max_y_axis = n_max_y

        # end - get proper y axis end
        dict_graph_configure = {'xs': lst_xs, 'ys': lst_series_data, 'labels': lst_series_label,
                                'line_color': lst_series_color}
        o_graph_source = ColumnDataSource(dict_graph_configure)
        # import datetime
        # time_strs = ['2019-07-11 10:00:00', '2019-07-11 10:15:00', '2019-07-11 10:30:00', '2019-07-11 10:45:00']
        # time_objs = [datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S') for time_str in time_strs]
        # data = dict(x=time_objs, y=[5, 4, 6, 5])

        idx_x_range = Range1d(start=lst_xs[0][0], end=lst_xs[0][-1])
        o_graph = figure(title=self.__g_sGraphTitle,
                         name='bar',
                         x_range=idx_x_range,
                         y_range=Range1d(start=0, end=self.__get_proper_round_up(n_max_y_axis)),
                         plot_height=self.__g_nGraphHeight,
                         # toolbar_location=None,
                         tools="pan,wheel_zoom,box_zoom,reset,save",  # select the tools we want
                         )

        # https://stackoverflow.com/questions/32652231/automatically-fit-bokeh-plot-to-screen
        o_graph.sizing_mode = 'scale_width'  # 'plot_height', 'plot_both'
        o_graph.add_layout(Legend(), 'below')  # place values: left, right, above, below or center
        o_graph.multi_line(xs='xs', ys='ys', legend_field='labels', source=o_graph_source, line_color='line_color',
                           line_width=5)
        # https://stackoverflow.com/questions/49282078/multiple-hovertools-for-different-lines-bokeh
        # https://stackoverflow.com/questions/38304753/multi-line-hover-in-bokeh
        # https://discourse.bokeh.org/t/unique-hover-tool-for-multiple-lines-on-same-graph/2821
        # https://stackoverflow.com/questions/50823204/bokeh-hovertool-shows
        # http://docs.bokeh.org/en/1.0.2/docs/user_guide/examples/tools_hover_tooltip_formatting.html
        o_graph.add_tools(HoverTool(show_arrow=False, line_policy='next', tooltips=[
            ('값', '$y{0,0}'), ('계열', '@labels'), ('x축', '$x{0,0}')
        ]))  # , mode='vline' -> 동일 X 값의 다른 계열도 동시에 표시
        # https://stackoverflow.com/questions/42303605/python-bokeh-how-to-change-range-of-datetime-axis
        # o_graph.scatter(date_log, df_sums['media_gross_cost_inc_vat'].tolist(), size=12, color="blue", alpha=0.5)
        # o_graph.vbar(x=date_log, top=df_tm_sums['media_gross_cost_inc_vat'].tolist(), width=0.8)
        o_graph.legend.location = "bottom_center"
        o_graph.legend.orientation = "horizontal"

        """# Vertical line
        vline = Span(location=10, dimension='height', line_color='red', line_width=3)
        # Horizontal line
        hline = Span(location=1, dimension='width', line_color='green', line_width=3)
        # start_date = time.mktime(datetime.date(2018, 3, 19).timetuple()) * 1000
        # vline = Span(location=start_date, dimension='height', line_color='red', line_width=3)
        o_graph.renderers.extend([vline, hline])"""

        # try:
        if self.__g_dictLabelOrientation.get(s_xlabel_orientation, False):
            f_inclined = self.__g_dictLabelOrientation[s_xlabel_orientation]
        # except KeyError:
        else:
            f_inclined = 0

        o_graph.xaxis.major_label_orientation = f_inclined
        return o_graph

    def draw_vertical_bar(self, n_max_y_axis):
        """
        build vertical stacked bar figures
        :param n_max_y_axis:
        :return:
        """
        # get series values
        lst_series_data = self.__g_dictSeriesValue[self.__g_nClassId]
        lst_series_label = self.__g_dictSeriesLabel[self.__g_nClassId]
        lst_series_color = self.__g_dictSeriesColor[self.__g_nClassId]
        # validate series info
        self.__validate_series_info()

        # construct data to visualize
        dict_data = {}
        n_idx = 0
        for lst_series_val in lst_series_data:
            dict_data[lst_series_label[n_idx]] = lst_series_val
            n_idx = n_idx + 1
        dict_data['x_lbl'] = self.__g_lstSeriesXLabel  # lst_x_lbl

        lst_series_lbl = lst_series_label
        # lst_series_lbl = list(dict_data.keys())
        # lst_series_lbl.remove('x_lbl')
        # print(lst_series_lbl)
        # print(dict_data['x_lbl'])

        # create data set to draw
        if n_max_y_axis is None:
            range_y = None
        else:
            range_y = Range1d(start=0, end=self.__get_proper_round_up(n_max_y_axis))
        # add plot
        o_graph = figure(
            x_range=dict_data['x_lbl'],
            y_range=range_y,
            plot_height=self.__g_nGraphHeight,
            title=self.__g_sGraphTitle,
            # toolbar_location=None,
            tools="pan,wheel_zoom,box_zoom,reset,save",
            tooltips=[("Name", "$name"), ("Val", "@$name{0,00}")]
        )

        # https://stackoverflow.com/questions/46730609/position-the-legend-outside-the-plot-area-with-bokeh
        o_graph.sizing_mode = 'scale_width'  # 'plot_height', 'plot_both'
        o_graph.add_layout(Legend(), 'below')  # place values: left, right, above, below or center
        o_graph.vbar_stack(
            lst_series_lbl,
            x='x_lbl',
            width=0.9,
            source=dict_data,
            legend_label=lst_series_lbl,
            color=lst_series_color  # lst_palette  # Dark2_8[0:len(dict_data)]  # assume maximum 9 PS source_mediums to display
        )

        o_graph.y_range.start = 0
        o_graph.x_range.range_padding = 0.1
        o_graph.xaxis.major_label_orientation = 1
        o_graph.xgrid.grid_line_color = None
        o_graph.legend.location = "bottom_center"
        o_graph.legend.orientation = "horizontal"
        return o_graph

    def draw_horizontal_bar(self):
        """
        draw categorical horizontal bar graph
        :return:
        """
        # get series values
        lst_series_data = self.__g_dictSeriesValue[self.__g_nClassId]
        lst_series_label = self.__g_dictSeriesLabel[self.__g_nClassId]
        lst_series_color = self.__g_dictSeriesColor[self.__g_nClassId]
        # validate series info
        self.__validate_series_info()

        # https://docs.bokeh.org/en/latest/docs/user_guide/categorical.html
        # lst_sku = ['Apples', 'Pears', 'Nectarines', 'Plums', 'Grapes', 'Strawberries']
        # lst_x_axis = list(dict_data.keys())  # set x axis label  # ['LLY', 'LY', 'LM', 'TM']

        # this creates [ ("Apples", "2015"), ("Apples", "2016"), ("Apples", "2017"), ("Pears", "2015), ... ]
        x = [(s_sku, year) for s_sku in self.__g_lstSeriesXLabel for year in lst_series_label]

        # this creates tuple (48586680, 67721311, 99707602, 0, 77480036, 73981937, 67232041, 0, 34710841, 43456264, ...)
        # counts = sum(zip(data['LLY'], data['LY'], data['LM'], data['TM']), ())  # like an hstack
        # begin - dynamically create serialized value list, instead of fixed sum(zip(), ()) method above
        n_iter_cnt = 0
        dict_tmp = {}
        # for s_series_title, lst_series in dict_data.items():
        for lst_single_data in lst_series_data:
            if n_iter_cnt == 0:  # create slot
                n_iter_cnt = n_iter_cnt + 1
                n_slot_idx = 0
                for n_val in lst_single_data:
                    dict_tmp[n_slot_idx] = [n_val]
                    n_slot_idx = n_slot_idx + 1
            else:  # allocate and accumulation
                n_slot_idx = 0
                for n_val in lst_single_data:
                    dict_tmp[n_slot_idx].append(n_val)
                    n_slot_idx = n_slot_idx + 1
        del lst_single_data
        # del s_series_title, lst_series

        lst_serialized_value = []
        for n_idx, lst_series in dict_tmp.items():
            for n_val in lst_series:
                lst_serialized_value.append(n_val)
        del dict_tmp

        # end - dynamically create serialized value list, instead of fixed sum(zip(), ()) method above
        source = ColumnDataSource(data=dict(x=x, counts=lst_serialized_value))
        # https://stackoverflow.com/questions/31496628/in-bokeh-how-do-i-add-tooltips-to-a-timeseries-chart-hover-tool
        o_graph = figure(x_range=FactorRange(*x),
                         plot_height=self.__g_nGraphHeight,
                         title=self.__g_sGraphTitle,
                         # toolbar_location=None,
                         tools="pan,wheel_zoom,box_zoom,reset,save",
                         tooltips=[("Series", "@x"), ("Val", "@counts{0,00}"), ])

        o_graph.sizing_mode = 'scale_width'
        # tup_bar_color = ('#D6E2DF', '#A4C8C1', '#6CBDAC', '#079476')
        o_graph.vbar(x='x', top='counts', width=0.7, source=source, line_color="white",
                     # use the palette to colormap based on the the x[1:2] values
                     fill_color=factor_cmap('x', palette=lst_series_color, factors=lst_series_label, start=1, end=2))
        o_graph.y_range.start = 0
        o_graph.x_range.range_padding = 0.1
        o_graph.xaxis.major_label_orientation = self.__g_dictLabelOrientation['i']
        # https://stackoverflow.com/questions/48631546/rotating-minor-tick-labels-for-categorical-bokeh-plot
        # o_graph.xaxis.subgroup_label_orientation = "normal"
        o_graph.xaxis.group_label_orientation = self.__g_dictLabelOrientation['i']  # 0.8
        o_graph.xgrid.grid_line_color = None
        return o_graph

    def __validate_series_info(self):
        n_lst_series_data_cnt = len(self.__g_dictSeriesValue[self.__g_nClassId])
        n_lst_series_label_cnt = len(self.__g_dictSeriesLabel[self.__g_nClassId])
        n_lst_series_color_cnt = len(self.__g_dictSeriesColor[self.__g_nClassId])
        if n_lst_series_data_cnt != n_lst_series_label_cnt != n_lst_series_color_cnt:
            raise Exception('invalid series info')
        return n_lst_series_color_cnt

    def __get_proper_round_up(self, x, place=None):
        if not place:
            if x >= 1:  # for integer
                n_digit_count = 0
                n_source = x
                while n_source > 0:
                    n_digit_count = n_digit_count + 1
                    n_source = n_source // 10
                if n_digit_count >= 5:
                    place = -(n_digit_count - 2)
                elif 5 > n_digit_count > 3:
                    place = -(n_digit_count - 2)
                elif n_digit_count == 3:
                    place = -1  # (n_digit_count - 1)
                else:
                    place = 0
            else:  # for decimal
                d = Decimal(str(x))
                n_decimal_cnt = d.as_tuple().exponent
                # print("The number of digits in the number are:", n_decimal_cnt)
                if n_decimal_cnt <= -6:
                    place = 3
                else:
                    place = 2
        return round(x + 5 * 10 ** (-1 * (place + 1)), place)
