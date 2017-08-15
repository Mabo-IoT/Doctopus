# -*- coding: utf-8 -*-
import redis
import time


class RedisWrapper:
    """
    包装 redis 库, 用 lua 脚本作为入队筛选机制

    用法：
    db = database_wrapper.RedisWrapper(conf)
    db.script_load(lua_file)
    db.enqueue(**kwargs)  #入队
    """

    def __init__(self, conf):
        """
        :param conf: dict, 包含 Redis 的 host, port, db
        """
        pool = redis.ConnectionPool(
            host=conf.get('host', 'localhost'),
            port=conf.get('port', 6379),
            db=conf.get('db', 0))
        self.__db = redis.StrictRedis(connection_pool=pool, socket_timeout=1)

        # 测试redis连通性
        self.test_connect()

    def test_connect(self):
        """
        初始化连接 Redis 数据库, 确保 redis 连接成功 
        :return: None
        """
        while True:
            try:
                self.__db.ping()
                return True
            except (ConnectionError, Exception) as e:
                #log.error('\n' + str(e) + '\n')
                time.sleep(2)
                continue

    def script_load(self, lua_script_file):
        """
        加载 Lua 脚本, 生成对应的 sha, 保存在类属性中
        :param lua_script_file: Lua file path
        :return: None
        """
        self.test_connect()
        with open(lua_script_file, 'r') as fn:
            script = fn.read()
            self.sha = self.__db.script_load(script)

    def enqueue(self, **kwargs):
        """
        将传入的参数传入 Lua 脚本进行处理，默认第一个值为 key
        :param kwargs: 位置参数
        :return: lua 脚本返回值
        """
        timestamp = kwargs.pop('timestamp')
        tags = kwargs.pop('tags')
        fields = kwargs.pop('fields')
        measurement = kwargs.pop('measurement')
        unit = kwargs.get('unit', 's')

        return self.__db.evalsha(self.sha, 1, tags, timestamp, fields, measurement, unit)

    def dequeue(self, key):
        """
        Remove and return the first item of the list ``data_queue``
        if ``data_queue`` is an empty list, block indefinitely
        """
        return self.__db.lpop(key)

    def get_len(self, key):
        """
        Return the length of the list ``data_queue``
        """
        return self.__db.llen(key)

    def queue_back(self, key, data):
        """
        Push the data onto the head of the list ``data_queue``
        """
        return self.__db.lpush(key, data)

    def flushdb(self):
        """
        Delete all keys in the current database
        """
        return self.__db.flushdb()

    def keys(self, pattern='*'):
        """
        Returns a list of keys matching ``pattern``
        """
        return self.__db.keys(pattern)

    def sadd(self, name, *values):
        """
        Add value(s) to set name
        :param name: set name
        :param values: set values
        :return: 
        """
        return self.__db.sadd(name, *values)

    def smembers(self, name):
        """
        Return all members of the set ``name``
        :param name: set name
        :return: smembers
        """
        return self.__db.smembers(name)

    def rpush(self, name, *values):
        """
        Push ``values`` onto the tail of the list ``name``
        :param name: key_name 
        :param values: 
        :return: 
        """
        return self.__db.rpush(name, *values)

    def expire(self, name, time):
        """
        Set an expire flag on key ``name`` for ``time`` seconds. ``time``
        can be represented by an integer or a Python timedelta object.
        """
        return self.__db.expire(name, time)
