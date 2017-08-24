# -*- coding: utf-8 -*-

import threading
import ctypes
import inspect
import time

Lock = threading.RLock()


class WatchDog:
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
        self.thread_set = None
        self.instance_set = None
        self.reload = False
        self.restart = False
        self.thread_real_time_names = set()
        self.check_restart_num = 0
        self.handle_restart_num = 0

    @staticmethod
    def _async_raise(tid, exctype):
        """
        Kill the specified thread
        raises the exception, performs cleanup if needed
        :param:
        tid: A single thread instance of ident, e.g: thread.ident
        exctype: Some system signals, SystemExit means kill
        """
        tid = ctypes.c_long(tid)
        if not inspect.isclass(exctype):
            exctype = type(exctype)
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
        if res == 0:
            print("invalid thread id")
        elif res != 1:
            """if it returns a number greater than one, you're in trouble,
            and you should call it again with exc=NULL to revert the effect"""
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
            print("PyThreadState_SetAsyncExc failed")

    def work(self, *args):
        """
        监控 check 和 handle 线程，并对命令进行反馈
        :param args:
            args[0]: dict, key:thread_name, value:thread_instance
            args[1]: dict, 线程间通信的 queue 集合
            args[2]: dict, key: name, value: class_instance
        :return:
        """
        self.thread_set, queue, self.instance_set = args
        thread_names = {thread.name for thread in self.thread_set.values()}

        while True:
            thread_real_time_names = set()
            for item in threading.enumerate():
                thread_real_time_names.add(item.name)

            self.thread_real_time_names = thread_real_time_names

            different = thread_real_time_names & thread_names

            if different != thread_names:
                dead_threads = thread_names - different
                self.re_start(dead_threads, queue)

            if self.reload or self.restart:
                self.kill(self.thread_set)

            time.sleep(10)

    def kill(self, threads):
        """
        kill some thread
        :param threads: thread set
        :return:
        """
        for thread in threads.values():
            self._async_raise(thread.ident, SystemExit)

            if thread.name == 'check':
                self.check_restart_num += 1
            else:
                self.handle_restart_num += 1

        self.restart = False

    def re_start(self, dead_threads, queue):
        """
        Restart dead thread
        :param dead_threads: dead threads name
        :param queue: Inter-thread communication queue
        :return:
        """
        instances = list()
        if self.reload:
            pass
            self.reload = False
        else:
            instances = [thread for thread in self.instance_set if thread.name in dead_threads]

        threads_set = dict()

        for instance in instances:
            worker = threading.Thread(target=instance.work, args=(queue,),
                                      name='%s' % instance.name)
            worker.setDaemon(True)
            worker.start()
            threads_set[instance.name] = worker

            self.thread_set.update(threads_set)
