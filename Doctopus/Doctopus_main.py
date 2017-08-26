# -*- coding: utf-8 -*-

import logging
import types
from abc import ABCMeta, abstractmethod

import pendulum

log = logging.getLogger("Doctopus.main")


class Check(object):
    __metaclass__ = ABCMeta

    def __init__(self, configuration):
        self.conf = configuration
        pass

    def work(self, queues, **kwargs):
        """
        put check's data in queues, so Handler can use
        :param queues: 
        :param kwargs: 
        :return: 
        """
        self.data_queue = data_queue = queues['data_queue']

        while True:
            raw_datas = self.user_check()

            if isinstance(raw_datas, types.GeneratorType):
                for raw_data in raw_datas:
                    # put data in queue so handler can take
                    data_queue.put(raw_data)

            else:
                # return data in queue so handler can take
                data_queue.put(raw_datas)

    @abstractmethod
    def user_check(self):
        """
        user-defined plugin to collect data
        :param self:
        :return:
        """
        pass


class Handler(object):
    __metaclass__ = ABCMeta

    def __init__(self, configuration):
        self.conf = configuration['user_conf']['handler']
        self.field_name_list = self.conf.get('field_name_list', [])
        self.table_name = self.conf.get('table_name', 'influxdb')
        self.unit = self.conf.get('unit', 's')

    def work(self, queues, **kwargs):
        while True:
            self.data_queue = data_queue = queues['data_queue']
            self.sender_pipe = queues['sender']
            raw_data = data_queue.get()

            if raw_data:
                processed_dicts = self.user_handle(raw_data)
                self.enque_prepare(processed_dicts)

            else:
                log.error('\nNo data is received')

    def enque_prepare(self, processed_dicts):
        """
        process data_dicts, then put data_dicts to queue so sender can take it         
        :param processed_dicts: 
        :return: 
        """
        data = None
        if isinstance(processed_dicts, (types.GeneratorType, list)):
            for processed_dict in processed_dicts:
                data = self.process_dict(processed_dict)

        elif isinstance(processed_dicts, dict):

            data = self.process_dict(processed_dicts)

        self.sender_pipe.put(data)

    def process_dict(self, processed_dict):
        """
        process dict to the format sender need
        :param processed_dict: 
        :return: 
        """
        table_name = self.table_name

        # make fields
        value_list = processed_dict.get('data_value')

        # user field list
        if not isinstance(value_list, dict):
            fields = dict(zip(self.field_name_list, value_list))
        else:
            fields = value_list

        # timestamp
        if 'unit' in fields.keys():
            # send to influxdb must has "unit"
            if processed_dict['unit'] == 's':
                timestamp = processed_dict.get('timestamp') or pendulum.now().int_timestamp
            else:
                timestamp = processed_dict.get('timestamp') or int(pendulum.now().float_timestamp * 1000000)
        else:
            # send to other db "unit" is not necessary
            timestamp = processed_dict.get('timestamp') or pendulum.now().int_timestamp

        # fields.update(influxdb_dict)

        # data to put in send
        data_dict = {
            "table_name": table_name,
            "fields": fields,
            "timestamp": timestamp
        }

        return data_dict

    @abstractmethod
    def user_handle(self, raw_data):
        """
        user should output a dict, which contain
        timestamp, (choose)
        tags,(choose)
        data_value,(must)
        measurement(must)
        :param raw_data: 
        :return: 
        """
        pass
