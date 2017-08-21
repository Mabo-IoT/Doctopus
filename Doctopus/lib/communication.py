# -*- coding: utf-8 -*-

import threading
from Doctopus.lib.database_wrapper import RedisWrapper
from Doctopus.lib.watchdog import WatchDog

Lock = threading.RLock()


class Communication:
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
        self.watchdog = WatchDog(conf)
        self.node = conf['node']
        self.data = None

    def work(self, *args):
        while True:
            # 获取外部命令，并处理
            command = self.check_order()

            if command == 'get_status':
                self.write_into_local()
            elif command == 'restart':
                self.watchdog.restart = True
            elif command == 'reload':
                self.watchdog.reload = True

            self.write_into_remote()

    def check_order(self):
        """
        Get outside command
        :return: None or order
        """
        if self.redis.get_len("order_name") > 0:
            return self.redis.dequeue("order_name")

    def write_into_local(self):
        """
        Write the data to local redis
        :param thread_real_time_names: currently surviving threads
        :return: None
        """
        status = {
            'node': self.node,
            'data': self.data,
            'log_error': self.read_log(),
            'check_restart_time': self.watchdog.check_restart_num,
            'handle_restart_time': self.watchdog.handle_restart_num,
            'real_time_thread_name': self.watchdog.thread_real_time_names
        }
        self.redis.sadd('status', status)
        self.redis.expire('status', 60 * 5)

    def write_into_remote(self):
        pass

    def read_log(self):
        pass
