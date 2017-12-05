# -*- coding: utf-8 -*-

import logging
import msgpack
import time
import sys

if sys.version_info[0] == 3 and sys.version_info[1] >= 5:
    from Doctopus.lib.communication import Communication
else:
    from Doctopus.lib.communication_2 import Communication
from Doctopus.lib.database_wrapper import InfluxdbWrapper, RedisWrapper
from Doctopus.utils.util import get_conf

log = logging.getLogger(__name__)


class Transport:
    def __init__(self, conf, redis_address):
        self.to_where = conf['send_to_where']
        self.redis = RedisWrapper(redis_address)
        self.data_original = None
        self.name = None
        self.communication = Communication(conf)

        if self.to_where == 'influxdb':
            self.db = InfluxdbWrapper(conf['influxdb'])

    def work(self, *args):
        while True:
            raw_data = self.unpack()

            data = self.pack(raw_data)
            if data:
                try:
                    self.send(data)
                except Exception as e:
                    log.error("\n%s", e)
                    self.reque_data()
                    time.sleep(3)

    def unpack(self):
        """
        Get data from redis and unpack it
        :return: dict
        """
        data_len = self.redis.get_len("data_queue")
        data = dict()

        if data_len > 0:
            self.data_original = self.redis.dequeue("data_queue")
            try:
                for k, v in msgpack.unpackb(self.data_original).items():
                    if v == True:
                        continue
                    else:
                        data[k.decode('utf-8')] = msgpack.unpackb(v, encoding='utf-8')
            except Exception as e:
                log.error("\n%s", e)
        else:
            log.info('redis have no data')
            time.sleep(5)
        return data

    def pack(self, data):
        """
        Converts the data to the format required for the corresponding database
        :param data: dict
        :return: the format data, usually dict
        """
        if data:
            if self.to_where == 'influxdb':
                try:
                    measurement = data['table_name']
                    tags = data['fields'].pop('tags')

                    unit = data['fields'].pop('unit')

                    fields = data['fields']
                    timestamp = data['time']

                    json_data = [
                        {
                            'measurement': measurement,
                            'tags': tags,
                            'time': timestamp,
                            'fields': fields,
                            'unit': unit
                        }
                    ]
                    return json_data
                except Exception as e:
                    log.error("\n%s", e)
            else:
                data['fields'].pop('tags')
                data['fields'].pop('unit')

    def send(self, data):
        """
        Sent to the corresponding database
        :param data: dict
        :return: None
        """
        if self.to_where == 'influxdb':
            time_precision = data[0].pop('unit')
            info = self.db.send(data, time_precision)
            self.communication.data[data[0]["measurement"]] = data
            if info:
                log.info('send data to inflxudb.{}, {}'.format(data[0]['measurement'], info))
            else:
                raise Exception("\nCan't connect influxdb")

    def reque_data(self):
        """
        return data to redis
        :return:
        """
        self.redis.queue_back('data_queue', self.data_original)

    def re_load(self):
        conf = get_conf()
        self.to_where = conf['send_to_where']
        self.data_original = None
        self.name = None

        if self.to_where == 'influxdb':
            self.db = InfluxdbWrapper(conf['influxdb'])
        return self
