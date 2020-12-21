# -*- coding: utf-8 -*-

import hashlib
import logging
import os
import platform
import threading

import gevent
from Doctopus.lib.database_wrapper import RedisWrapper
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
        self.watchdog = WatchDog(conf)
        self.app = conf['application']
        self.data = dict()
        self._name = 'communication'
        self.log = list()
        self.hash = None

        # 20-12-14 zhy: windows启动异常，communication中没有self.paths参数
        self.__init_paths()
        # 重启刷新缓存
        self.flush_data()

    def __init_paths(self):
        if self.app == "ziyan":
            self.paths = ['./conf/conf.toml', './lua/enque_script.lua', './plugins/your_plugin.py']
        elif self.app == "chitu":
            self.paths = ['./conf/conf.toml']
        else:
            self.paths = []


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
            gevent.joinall(
                [gevent.spawn(self.handle),
                 gevent.spawn(self.monitor)])
        else:
            gevent.joinall([
                gevent.spawn(self.handle),
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

            gevent.sleep(0.5)

    def check_order(self):
        """
        Get outside command
        :return: None or order
        """
        try:
            if self.redis.get_len("order_name") > 0:
                return self.redis.dequeue("order_name")
        except Exception as err:
            log.error(err)

    def write_into_local(self):
        """
        Write the data to local redis
        :return: None
        """
        if self.app == 'ziyan':
            status = {
                'data': self.data,
                'log': self.log,
                'check_restart_time': self.watchdog.check_restart_num,
                'handle_restart_time': self.watchdog.handle_restart_num,
                'real_time_thread_name': self.watchdog.thread_real_time_names
            }
        else:
            status = {
                'data': self.data,
                'log': self.log,
                'transport_restart_time': self.watchdog.transport_restart_num,
                'real_time_thread_name': self.watchdog.thread_real_time_names
            }
        try:
            self.redis.sadd('status', status)
            self.redis.expire('status', 60 * 5)
        except Exception as err:
            log.error(err)

    def flush_data(self):
        """
        Delete the existing key "status"
        :return:
        """
        try:
            if self.redis.exists("status"):
                self.redis.delete("status")
        except Exception as err:
            log.error(err)

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
        self.app = conf['application']

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
