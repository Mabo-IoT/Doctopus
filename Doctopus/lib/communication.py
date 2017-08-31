# -*- coding: utf-8 -*-

import logging
import threading
import asyncio

from Doctopus.lib.database_wrapper import RedisWrapper, EtcdWrapper
from Doctopus.lib.watchdog import WatchDog

Lock = threading.RLock()
log = logging.getLogger("Doctopus.communication")


class Communication:
    INSTANCE = None

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
                Lock.acquire()
                if not cls.INSTANCE:
                    _instance = object.__new__(cls)
                    cls.INSTANCE = _instance
            finally:
                Lock.release()
        return cls.INSTANCE

    def __init__(self, conf):
        self.redis = RedisWrapper(conf['redis'])
        self.etcd = EtcdWrapper(conf['etcd'])
        self.watchdog = WatchDog(conf)
        self.node = conf['node']
        self.ip = conf['local_ip']
        self.app = conf['application']
        self.paths = conf['paths']
        self.data = None
        self.name = None
        self.log = list()

        # 重启刷新缓存
        self.flush_data()

    def work(self, *args):
        """
        获取 event loop，并启动异步循环
        :param args:
        :return:
        """
        loop = asyncio.SelectorEventLoop()
        asyncio.set_event_loop(loop)
        loop.call_soon_threadsafe(loop.create_task, self.handle())
        loop.call_soon_threadsafe(loop.create_task, self.write_into_remote())
        loop.run_forever()

    async def handle(self):
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

            elif command == b'upload':

                for path in self.paths:
                    self.upload(path)
            await asyncio.sleep(0.5)

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
        status = {
            'node': self.node,
            'data': self.data,
            'log': self.log,
            'check_restart_time': self.watchdog.check_restart_num,
            'handle_restart_time': self.watchdog.handle_restart_num,
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

    async def write_into_remote(self):
        """
        异步方法，每10分钟向服务器 etcd 中注册当前状态
        :return:
        """
        while True:
            status = {
                'node': self.node,
                'ip': self.ip,
                'data': self.data,
                'log': self.log,
                'check_restart_time': self.watchdog.check_restart_num,
                'handle_restart_time': self.watchdog.handle_restart_num,
                'real_time_thread_name': self.watchdog.thread_real_time_names
            }

            key = "/nodes/" + self.node + "/" + self.app + "/status"
            try:
                self.etcd.write(key, status)
                log.debug("\nkey:%s\ndata:%s\n", key, status)
            except Exception as e:
                log.error("\n%s", e)

            await asyncio.sleep(10 * 60)

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
