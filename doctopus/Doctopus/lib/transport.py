# -*- coding: utf-8 -*-

import logging
import sys
import time
import traceback
from datetime import datetime
from queue import Queue
from influxdb.exceptions import InfluxDBClientError
import msgpack
from redis import exceptions

if sys.version_info[0] == 3 and sys.version_info[1] >= 5:
    from Doctopus.lib.communication import Communication
else:
    from Doctopus.lib.communication_2 import Communication

from Doctopus.lib.database_wrapper import InfluxdbWrapper, RedisWrapper
from Doctopus.lib.kafka_wrapper import KafkaWrapper
from Doctopus.lib.mqtt_wrapper import MqttWrapper
from Doctopus.utils.util import get_conf

log = logging.getLogger(__name__)


class Transport:
    def __init__(self, conf, redis_address=None):
        self.to_where = conf['send_to_where']

        self.data_original = None
        self.name = None
        self.communication = Communication(conf)

        # Redis conf
        self.redis = RedisWrapper(redis_address)
        self.group = conf['data_stream']['group']
        self.consumer = conf['data_stream']['consumer']
        # create group for data_stream
        self.redis.addGroup(self.group)

        if self.to_where == 'influxdb':
            self.db = InfluxdbWrapper(conf['influxdb'])
        elif self.to_where == 'kafka':
            self.db = self.initKafka(conf['kafka'])
        elif self.to_where == 'mqtt':
            self.mqtt_conf = conf.get('mqtt', dict())
            self.mqtt_put_queue = Queue()
            self.mqtt = MqttWrapper(self.mqtt_conf)

    def initKafka(self, conf):
        while True:
            try:
                client = KafkaWrapper(conf)
                return client

            except Exception as err:
                log.exception(err)
                log.error("Can't init Kafka client, try again...")
                time.sleep(1)

    def work(self, *args):
        while True:
            # get and decompress data
            try:
                bin_data = self.getData()
                raw_data = self.unpack(bin_data)
                log.debug('Data from redis: {}'.format(raw_data))
            except exceptions.ResponseError as e:
                # NOGROUP for data_stream, recreate it.
                if "NOGROUP" in str(e):
                    log.error('{}, recreate group: {}'.format(
                        str(e), self.group))
                    self.redis.addGroup(self.group)
                raw_data = None
            except Exception as err:
                log.exception(err)
                raw_data = None

            # compress and send data
            if raw_data:
                data = self.pack(raw_data["data"])
                try:
                    # send data and ack data id
                    log.debug("Send data to {}.".format(self.to_where))
                    self.send(data)
                    log.debug("Redis ack data.")
                    self.redis.ack(self.group, raw_data["id"])
                except Exception as err:
                    log.exception(err)
                    time.sleep(3)

    def pending(self, *args):
        while True:
            try:
                pending_data = self.getPendingData()

            except exceptions.ResponseError as err:
                # NOGROUP for data_stream, recreate it.
                if "NOGROUP" in str(err):
                    log.exception(err)
                    self.redis.addGroup(self.group)
                pending_data = []
            except Exception as err:
                log.exception(err)
                pending_data = []

            if pending_data:
                for d in pending_data:
                    raw_data = self.unpack(d)
                    if raw_data:
                        data = self.pack(raw_data["data"])
                        try:
                            # send data and ack data id
                            log.debug("Send pending data to {}".format(
                                self.to_where))
                            self.send(data)
                            log.debug("Redis ack pending data.")
                            self.redis.ack(self.group, raw_data["id"])
                        except Exception as err:
                            log.exception(err)
                            log.debug(f'Err data is: {data}')
                            time.sleep(3)
            else:
                time.sleep(5)
                log.debug("No pending data.")

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
                    raw_data[k.decode('utf-8')] = msgpack.unpackb(
                        v, encoding='utf-8')
                data["data"] = raw_data
            except Exception as err:
                traceback.print_exc()
                log.exception(err)
        else:
            log.info('Redis have no new data.')
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

                    json_data = [{
                        'measurement': measurement,
                        'tags': tags,
                        'time': timestamp,
                        'fields': fields,
                        'unit': unit
                    }]
                    return json_data
                except Exception as err:
                    log.exception(err)
            elif self.to_where == 'kafka':
                try:
                    json_data = self.db.pack(data)
                    return json_data
                except Exception as err:
                    traceback.print_exc()
                    log.exception(err)
            elif self.to_where == 'mqtt':
                try:
                    schema, table = data.get('table_name').split('.')
                    data['fields'].pop('tags', None)
                    data['fields'].pop('unit', None)
                    data.pop('table_name', None)

                    fields = data['fields']
                    ts = data['time']
                    timestamp = datetime.fromtimestamp(ts).strftime(
                        "%Y-%m-%d %H:%M:%S")

                    json_data = {
                        "timestamp": timestamp,
                        "schema": schema,
                        "table": table,
                        "deviceid": table,
                        "fields": fields
                    }
                    return json_data
                except Exception as err:
                    raise err
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
            try:
                info = self.db.send(data, time_precision)
            except InfluxDBClientError as e:
                timestamp = data[0]['time'] / 1000000
                t_string = datetime.utcfromtimestamp(
                    timestamp).strftime('%Y-%m-%d %H:%M:%S')
                # 2021-07-19 zhy: 为防止超过保留策略的数据导致 chitu 传数据卡死.
                if 'points beyond retention policy dropped' in str(e):
                    log.warning('Data beyond influx retention policy, timestamp is: {}, means {}'.format(
                        timestamp, t_string))
                    log.warning('Data is: {}'.format(data))
                    log.warning('Drop it.')
                    info = 'Drop data because data beyond influx retention policy.'
                # https://10.7.0.117:9091/mabo_group/base_application/doctopus/issues/3
                # 2021-07-19 zhy: 为防止数据因未知原因导致parse错误, drop掉. 防止卡死.
                elif 'invalid field format' in str(e):
                    log.warning('Data parse error, influx can`t receive it, timestamp is: {}, means {}'.format(
                        timestamp, t_string))
                    log.warning('Data is: {}'.format(data))
                    log.warning('Drop it.')
                    info = 'Drop data because parse error, influx can`t receive it.'
                else:
                    raise e

            self.communication.data[data[0]["measurement"]] = data
            if info:
                log.info('Send data to inflxudb.{}, {}'.format(
                    data[0]['measurement'], info))
            else:
                raise Exception("\nCan't connect influxdb")

        elif self.to_where == 'kafka':
            try:
                log.debug("Stuck here in send kafka data.")
                self.db.sendMessage(data)
                log.info('Send data to kafka: {}'.format(
                    data["dims"]["data_name"]))
            except Exception as e:
                raise e
        elif self.to_where == 'mqtt':
            self.mqtt_put_queue.put(data)
            try:
                self.mqtt.pubMessage(self.mqtt_put_queue)
                log.info('Publish data to MQTT topics({}): {}'.format(
                    self.mqtt_conf.get('topics', list()), data))
            except Exception as err:
                log.exception(err)

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
            data: [dict{"id":string, "data":bytes}]
                  dict{id:string, data:bytes}
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
        elif self.to_where == 'mqtt':
            self.mqtt_conf = conf.get('mqtt', dict())
            self.mqtt = MqttWrapper(self.mqtt_conf)
            self.mqtt_put_queue = Queue()
        return self
