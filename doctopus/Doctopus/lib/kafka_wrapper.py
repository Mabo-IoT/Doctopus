# -*- coding: utf-8 -*-
import json
import logging

from kafka import KafkaProducer

log = logging.getLogger(__name__)

#  class Singleton(type):
#      _instances = {}
#
#      def __call__(cls, *args, **kwargs):
#          if cls not in cls._instances:
#              cls._instances[cls] = super(Singleton,
#                                          cls).__call__(*args, **kwargs)
#          return cls._instances[cls]


class KafkaWrapper:
    """
    Producer is thread safe,
    it will start a back thread automatically to send messages.
    """
    def __init__(self, conf):
        self.conf: dict = conf
        self.topic: str = conf.get("topic", "default")
        self.bootstrap_servers: str = conf.get("bootstrap_servers",
                                               "localhost:9092")
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        except Exception as e:
            raise e

    def sendMessage(self, msg):
        """
        send msg to kafka brokers
        """
        try:
            log.debug("send data to topic {}".format(self.topic))
            future = self.producer.send(self.topic, msg)
            # use future get to ensure msg was sent successfully
            # or raise exception
            log.debug("get kafka future")
            future.get(timeout=10)
        except Exception as e:
            raise e

    def pack(self, data):
        """
        Pack redis data to kafka msg
        Args:
            data: redis msg dict i.e.
                {
                    "table_name": string, # measurement name
                    "fields": {
                        "...": fields, # device channels
                        "tags":{
                            "eqpt_no": # string, device eqpt_no
                            "...": "..." # others tags
                        }
                        "unit": string,# timestamp unit s,ms,us
                    time: int, # timestamp
                    }
                }
        Returns:
            msg: kafka msg i.e.
            {
                "ts": ts,   # timestamp, default unit is s
                "ip": "10.203.122.181", # local machine ip
                "dataid": 3605, #
                "org": 3101, # org id is default 3101
                "dims": {
                    "data_name": string, # measurement name
                    "eapt_no": string,   # tags eapt_no
                    "...": "...",        # more tags
                 },
                "vals": {
                    "...": "...", # fields
                }
            }
        Raises:
            Exception which redis msg data is not valid
        """
        try:
            measurement = data['table_name']
            tags = data['fields'].pop('tags')
            unit = data['fields'].pop('unit')
            fields = data['fields']
            timestamp = data['time']

            msg = {}
            # dims
            dims = {}
            dims["data_name"] = measurement
            dims.update(tags)
            # construct msg
            msg["ip"] = self.conf["ip"]
            msg["org"] = self.conf["org"]
            msg["dataid"] = self.conf["dataid"]
            msg["dims"] = dims
            msg["vals"] = fields

            # time
            if unit == 's':
                msg["ts"] = timestamp
            elif unit == 'm':
                msg["ts"] = timestamp / 1000
            elif unit == 'u':
                msg["ts"] = timestamp / 1000000

            msg["vals"]["time"] = msg["ts"]
            # fields["time"] = msg["ts"] #add by wmm 2020/7/7
            # msg["vals"].update(fields) #add by wmm 2020/7/7

        except Exception as e:
            raise e

        return msg
