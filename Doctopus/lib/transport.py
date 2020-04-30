# -*- coding: utf-8 -*-

import logging
import msgpack
import time
import sys
import traceback

from redis import exceptions
if sys.version_info[0] == 3 and sys.version_info[1] >= 5:
    from Doctopus.lib.communication import Communication
else:
    from Doctopus.lib.communication_2 import Communication
from Doctopus.lib.database_wrapper import InfluxdbWrapper, RedisWrapper
from Doctopus.lib.kafka_wrapper import KafkaWrapper
from Doctopus.utils.util import get_conf

log = logging.getLogger(__name__)


class Transport:
    def __init__(self, conf, redis_address):
        self.to_where = conf['send_to_where']
        self.group = conf['data_stream']['group']
        self.consumer = conf['data_stream']['consumer']

        self.redis = RedisWrapper(redis_address)
        self.data_original = None
        self.name = None
        self.communication = Communication(conf)

        # create group for data_stream
        self.redis.addGroup(self.group)

        if self.to_where == 'influxdb':
            self.db = InfluxdbWrapper(conf['influxdb'])
        elif self.to_where == 'kafka':
            self.db = self.initKafka(conf['kafka'])

    def initKafka(self, conf):
        while True:
            try:
                client = KafkaWrapper(conf)
                return client

            except Exception as e:
                log.error(e)
                time.sleep(1)
                log.error("can't init kafka client, try again!")

    def work(self, *args):
        while True:
            try:
                raw_data = self.getData()
                log.debug(raw_data)
                raw_data = self.unpack(raw_data)
                log.debug(raw_data)

            except exceptions.ResponseError as e:
                # NOGROUP for data_stream, recreate it.
                if "NOGROUP" in str(e):
                    log.error(str(e) + " Recreate group".format(self.group))
                    self.redis.addGroup(self.group)
                raw_data = None
            except Exception as e:
                log.error(e)
                raw_data = None

            if raw_data:
                data = self.pack(raw_data["data"])
                try:
                    # send data and ack data id
                    log.debug("send data")
                    self.send(data)
                    log.debug("redis ack data")
                    self.redis.ack(self.group, raw_data["id"])

                except Exception as e:
                    log.error("\n%s", e)
                    time.sleep(3)

    def pending(self, *args):
        while True:
            try:
                pending_data = self.getPendingData()

            except exceptions.ResponseError as e:
                # NOGROUP for data_stream, recreate it.
                if "NOGROUP" in str(e):
                    log.error(e)
                    self.redis.addGroup(self.group)
                pending_data = []
            except Exception as e:
                log.error(e)
                pending_data = []

            if pending_data:
                for d in pending_data:
                    raw_data = self.unpack(d)
                    if raw_data:
                        data = self.pack(raw_data["data"])
                        try:
                            # send data and ack data id
                            log.debug("send pending data")
                            self.send(data)
                            log.debug("redis ack pending data")
                            self.redis.ack(self.group, raw_data["id"])
                            log.info("send pending_data ~")
                        except Exception as e:
                            log.error("\n%s", e)
                            time.sleep(3)
            else:
                time.sleep(5)
                log.debug("no pending data~")

    def unpack(self, data):
        """
        Get data from redis and unpack it
        :return: dict
        """
        # data_len = self.redis.get_len("data_queue")
        if data:
            try:
                data["data"] = msgpack.unpackb(data["data"])
                raw_data = data["data"]
                for k in list(raw_data.keys()):
                    v = raw_data[k]
                    raw_data.pop(k)
                    raw_data[k.decode('utf-8')
                             ] = msgpack.unpackb(v, encoding='utf-8')
                data["data"] = raw_data
            except Exception as e:
                traceback.print_exc()
                log.error("\n%s", e)
        else:
            log.info('redis have no new data')
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

            elif self.to_where == 'kafka':
                try:
                    json_data = self.db.pack(data)
                    return json_data
                except Exception as e:
                    traceback.print_exc()
                    log.error(e)
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
                log.info('send data to inflxudb.{}, {}'.format(
                    data[0]['measurement'], info))
            else:
                raise Exception("\nCan't connect influxdb")

        elif self.to_where == 'kafka':
            try:
                log.debug("stuck here in send kafka data")
                self.db.sendMessage(data)
                log.info('send data to kafka {}'.format(
                    data["dims"]["data_name"]))
            except Exception as e:
                raise e

    def getData(self):
        """Get data from data_stream
        return:
            data: dict; {id:string, data:bytes}
        """
        data = self.redis.readGroup(self.group, self.consumer)
        if not data:
            return None
        else:
            id, raw = data[0][1][0]
            return {
                "id": id.decode(),
                "data": raw[b'data'],
            }

    def getPendingData(self):
        """Get pending data from data_stream

        return:
            data: [dict{"id":string, "data":bytes}]; dict{id:string, data:bytes}
        """
        data = self.redis.readPending(self.group, self.consumer, 0)
        res = []
        if not data:
            return res
        else:
            for v in data[0][1]:
                id, raw = v
                res.append({"id": id.decode(), "data": raw[b'data']})
            return res

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
        elif self.to_where == 'kafka':
            self.db = KafkaWrapper(conf['kafka'])
        return self
