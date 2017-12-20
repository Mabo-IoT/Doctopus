# -*- coding: utf-8 -*-

import logging
import threading
import json
import gevent
import os
import hashlib
import platform

from Doctopus.lib.database_wrapper import RedisWrapper, EtcdWrapper
from Doctopus.lib.watchdog import WatchDog
from Doctopus.utils.util import get_conf

log = logging.getLogger(__name__)


class Communication(object):
    INSTANCE = None
    Lock = threading.RLock()

    def __new__(cls, *args, **kwargs):
        """
        单例模式的 double check 确保线程安全，增加缓冲标量
        确保第一次初始化完全
        :param args:
        :param kwargs:
        :return:
        """
        _instance = None
        if not cls.INSTANCE:
            try:
                cls.Lock.acquire()
                if not cls.INSTANCE:
                    _instance = object.__new__(cls)
                    cls.INSTANCE = _instance
            finally:
                cls.Lock.release()
        return cls.INSTANCE

    def __init__(self, conf):
        self.redis = RedisWrapper(conf['redis'])
        self.etcd = EtcdWrapper(conf['etcd'])
        self.etcd_interval_time = conf['etcd']['interval']
        self.watchdog = WatchDog(conf)
        self.node = conf['node']
        self.ip = conf['local_ip']
        self.app = conf['application']
        self.paths = conf['paths']
        self.data = dict()
        self._name = 'communication'
        self.log = list()
        self.hash = None

        # 重启刷新缓存
        self.flush_data()

    @property
    def name(self):
        return self._name

    def work(self, *args):
        """
        获取 event loop，并启动异步循环
        :param args:
        :return:
        """
        if platform.system() == "Windows":
            gevent.joinall([
                gevent.spawn(self.handle),
                gevent.spawn(self.write_into_remote),
                gevent.spawn(self.monitor)
            ])
        else:
            gevent.joinall([
                gevent.spawn(self.handle),
                gevent.spawn(self.write_into_remote)
            ])

    def handle(self):
        """
        异步方法，监听 redis 给出的命令
        :return:
        """
        while True:
            # 获取外部命令，并处理
            command = self.check_order()

            if command == b'get_status':
                self.write_into_local()

            elif command == b'restart':
                self.watchdog.restart = True
                self.flush_data()

            elif command == b'reload':
                self.watchdog.reload = True
                self.flush_data()
                self.re_load()

            elif command == b'upload':

                for path in self.paths:
                    self.upload(path)
            gevent.sleep(0.5)

    def check_order(self):
        """
        Get outside command
        :return: None or order
        """
        try:
            if self.redis.get_len("order_name") > 0:
                return self.redis.dequeue("order_name")
        except Exception as e:
            log.error("\n%s", e)

    def write_into_local(self):
        """
        Write the data to local redis
        :return: None
        """
        if self.app == 'ziyan':
            status = {
                'node': self.node,
                'data': self.data,
                'log': self.log,
                'check_restart_time': self.watchdog.check_restart_num,
                'handle_restart_time': self.watchdog.handle_restart_num,
                'real_time_thread_name': self.watchdog.thread_real_time_names
            }
        else:
            status = {
                'node': self.node,
                'data': self.data,
                'log': self.log,
                'transport_restart_time': self.watchdog.transport_restart_num,
                'real_time_thread_name': self.watchdog.thread_real_time_names
            }
        try:
            self.redis.sadd('status', status)
            self.redis.expire('status', 60 * 5)
        except Exception as e:
            log.error("\n%s", e)

    def upload(self, path):
        """
        upload specific path file
        :param path:
        :return:
        """
        key = "/nodes/" + self.node + "/" + self.app
        if 'toml' in path:
            key += '/conf'
        elif 'py' in path:
            key += '/code'
        elif 'lua' in path:
            key += '/lua'

        try:
            with open(path, 'rb') as f:
                self.etcd.write(key, f.read())

        except Exception as e:
            log.error("\n%s", e)

    def flush_data(self):
        """
        Delete the existing key "status"
        :return:
        """
        try:
            if self.redis.exists("status"):
                self.redis.delete("status")
        except Exception as e:
            log.error("\n%s", e)

    def write_into_remote(self):
        """
        异步方法，每10分钟向服务器 etcd 中注册当前状态
        :return:
        """
        while True:
            if self.app == 'ziyan':
                status = {
                    'node': self.node,
                    'data': self.data,
                    'log': self.log,
                    'check_restart_time': self.watchdog.check_restart_num,
                    'handle_restart_time': self.watchdog.handle_restart_num,
                    'real_time_thread_name': list(self.watchdog.thread_real_time_names)
                }
            else:
                status = {
                    'node': self.node,
                    'data': self.data,
                    'log': self.log,
                    'transport_restart_time': self.watchdog.transport_restart_num,
                    'real_time_thread_name': list(self.watchdog.thread_real_time_names)
                }

            key = "/nodes/" + self.node + "/" + self.app + "/status"
            try:
                self.etcd.write(key, json.dumps(status))
                log.debug("\nkey: %s\ndata: %s\n", key, status)
            except Exception as e:
                log.error("\n%s", e)

            gevent.sleep(self.etcd_interval_time)

    def enqueue_log(self, msg):
        """
        保存定长的历史 log 日志
        :param msg: str, log
        :return:
        """
        if len(self.log) < 10:
            self.log.append(msg)
        else:
            self.log.pop(0)
            self.log.append(msg)

    def re_load(self):
        conf = get_conf()
        self.node = conf['node']
        self.ip = conf['local_ip']
        self.app = conf['application']
        self.paths = conf['paths']
        self.etcd_interval_time = conf['etcd']['interval']

    def monitor(self):
        while True:
            data = ""
            for file in self.paths:
                data += str(os.stat(file).st_mtime)
            sha = hashlib.sha1(data.encode()).hexdigest()
            if not self.hash:
                self.hash = sha
            elif self.hash != sha:
                self.hash = sha
                self.redis.rpush("order_name", "reload")
            gevent.sleep(30)
