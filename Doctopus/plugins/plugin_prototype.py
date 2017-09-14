# -*- coding: utf-8 -*-
import time
from logging import getLogger

from Doctopus.Doctopus_main import Check, Handler

log = getLogger('Doctopus.plugins')


class MyCheck(Check):
    def __init__(self, configuration):
        super(MyCheck, self).__init__(configuration=configuration)

    def user_check(self):
        """

        :param command: user defined parameter.
        :return: the data you requested.
        """
        data = 'check的data'
        log.debug('%s', data)
        time.sleep(2)
        yield data


class MyHandler(Handler):
    def __init__(self, configuration):
        super(MyHandler, self).__init__(configuration=configuration)

    def user_handle(self, raw_data):
        """
        用户须输出一个dict，可以填写一下键值，也可以不填写
        timestamp， 从数据中处理得到的时间戳（整形?）
        tags, 根据数据得到的tag
        data_value 数据拼接形成的 list 或者 dict，如果为 list，则上层框架
         对 list 与 field_name_list 自动组合；如果为 dict，则不处理，认为该数据
         已经指定表名
        measurement 根据数据类型得到的 influxdb表名

        e.g:
        list:
        {'data_value':[list] , required
        'tags':[dict],        optional
        'table_name',[str]   optional
        'timestamp',int}      optional

        dict：
        {'data_value':{'fieldname': value} , required
        'tags':[dict],        optional
        'table_name',[str]   optional
        'timestamp',int}      optional

        :param raw_data: 
        :return: 
        """
        # exmple.
        # 数据经过处理之后生成 value_list
        log.debug('%s', raw_data)
        data_value_list = [raw_data]

        tags = {'user_defined_tag': 'data_ralated_tag'}

        # user 可以在handle里自己按数据格式制定tags
        user_postprocessed = {'data_value': data_value_list,
                              'tags': tags, }
        yield user_postprocessed
