# -*- coding: utf-8 -*-

from logging import getLogger

import msgpack
import pendulum
import sys

if sys.version_info[0] == 3 and sys.version_info[1] >= 5:
    from Doctopus.lib.communication import Communication
else:
    from Doctopus.lib.communication_2 import Communication
from Doctopus.lib.database_wrapper import RedisWrapper

log = getLogger(__name__)


class Sender(object):
    """
    send data to redis and watchdog
    """

    def __init__(self, configuration):

        self.redis_conf = configuration['redis']
        self.conf = configuration['sender']
        self.lua_path = self.conf['lua_path']

        self.db = RedisWrapper(self.redis_conf)
        self.db.script_load(self.lua_path)

        # log format
        self.enque_log_flag = self.conf['enque_log']
        self.log_format = '\ntable_name: {}\nfields: {}\ntimestamp: {}\n'

        # init communication class (singleinstance)
        self.communication = Communication(configuration)

        self.name = None

    def work(self, queue, **kwargs):
        """
        send data to redis and watchdog
        :param queue: 
        :param kwargs: 
        :return: 
        """
        sender_pipe = queue['sender']
        while True:
            data = sender_pipe.get()
            # pack and send data to redis and watchdog
            self.pack(data)
            self.send_to_communication(data)

    def pack(self, data):
        """
        pack data and send data to redis
        :param data: 
        :return: 
        """
        table_name = data['table_name']
        fields = data['fields']
        timestamp = data['timestamp']

        if 'unit' in fields.keys():
            if fields['unit'] == 's':
                date_time = pendulum.from_timestamp(timestamp, tz='Asia/Shanghai').to_datetime_string()
            else:
                date_time = pendulum.from_timestamp(timestamp / 1000000, tz='Asia/Shanghai').to_datetime_string()
        else:
            date_time = pendulum.from_timestamp(timestamp, tz='Asia/Shanghai').to_datetime_string()

        log_str = self.log_format.format(table_name, fields, date_time)
        # show log or not
        if self.enque_log_flag:
            log.info(log_str)
        # pack data by msgpack ready to send to redis
        table_name = msgpack.packb(table_name)
        fields = msgpack.packb(fields)
        timestamp = msgpack.packb(timestamp)
        # send data to redis
        try:
            lua_info = self.db.enqueue(table_name=table_name, fields=fields, timestamp=timestamp)
            log.info('\n' + lua_info.decode())
        except Exception as e:
            log.error("\n%s", e)

    def send_to_communication(self, data):
        """
        send data to communication instance(singleinstance)
        :param data: 
        :return: 
        """
        self.communication.data[data["table_name"]] = data
