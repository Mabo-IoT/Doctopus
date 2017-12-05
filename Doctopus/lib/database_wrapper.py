# -*- coding: utf-8 -*-
import logging
import time

import redis
import requests
from influxdb import InfluxDBClient
from etcd import Client

log = logging.getLogger(__name__)


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
            except Exception as e:
                log.error('\n%s', e)
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
        fields = kwargs.pop('fields')
        table_name = kwargs.pop('table_name')

        return self.__db.evalsha(self.sha, 1, table_name, fields, timestamp)

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

    def hset(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name``
        Returns 1 if HSET created a new field, otherwise 0
        """
        return self.__db.hset(name, key, value)

    def hgetall(self, name):
        "Return a Python dict of the hash's name/value pairs"
        return self.__db.hgetall(name)

    def exists(self, name):
        "Returns a boolean indicating whether key ``name`` exists"
        return self.__db.exists(name)

    def delete(self, *names):
        "Delete one or more keys specified by ``names``"
        return self.__db.delete(*names)


class InfluxdbWrapper:
    """
    包装 influxdb 库

    用法：
    josn_data = [
        {
            "measurement": "cpu_load_short",
            "tags": {
                "host": "server01",
                "region": "us-west"
            },
            "time": "2009-11-10T23:00:00Z",
            "fields": {
                "value": 0.64
            }
        }
    ]
    db = InfluxdbWrapper('localhost', 8086, 'root', 'root', db) or InfluxdbWrapper(conf)
    db.send(josn_data, retention_policy='specify')
    """

    def __init__(self, *args, **kwargs):
        if args and len(args) == 5:
            self.__db = InfluxDBClient(
                host=args[0],
                port=args[1],
                username=args[2],
                password=args[3],
                database=args[4],
                timeout=10
            )
            self.conf = args

        elif args and isinstance(args[0], dict):
            self.__db = InfluxDBClient(
                host=args[0].get('host', 'localhost'),
                port=args[0].get('port', 8086),
                username=args[0]['username'],
                password=args[0]['password'],
                database=args[0]['db'],
                timeout=args[0].get('timeout', 10)
            )
            self.conf = args[0]

        elif kwargs:
            self.__db = InfluxDBClient(
                host=kwargs.get('host', 'localhost'),
                port=kwargs.get('port', 8086),
                username=kwargs['username'],
                password=kwargs['password'],
                database=kwargs['db'],
                timeout=kwargs.get('timeout', 10)
            )
            self.conf = kwargs

        else:
            # log.error('No influxdb address')
            pass

        # 测试 influxdb 连通性
        self.test_connect()

    def test_connect(self):
        """
        初始化连接 Influxdb 数据库, 确保 Influxdb 连接成功
        :return: None
        """
        i = 0
        while True:
            try:
                self.query("show measurements limit 1")
                return True
            except Exception as e:
                i += 1
                if i > 10:
                    return False
                log.error("\n%s", e)
                time.sleep(2)

    def send(self, json_body, time_precision='s', database=None, retention_policy=None):
        """
        Write to multiple time series names
        :param json_body:  list of dictionaries, each dictionary represents a point,
                            the list of points to be written in the database
        :param time_precision: database time precision, default is second
        :param database: str,  the database to write the points to. Defaults to the client’s current database
        :param retention_policy: str, the retention policy for the points. Defaults to None
        :return: bool
        """
        if self.test_connect():
            return self.__db.write_points(json_body, time_precision=time_precision,
                                          database=database, retention_policy=retention_policy)
        else:
            return False

    def swith_database(self, database):
        """
        Change the client’s database.
        :param database: str, database name
        :return: None
        """
        self.__db.switch_database(database)

    def query(self, query):
        """
        Send a query to Influxdb
        :param query: str, SQL-like query statement
        :return: always return a list
        """
        return self.__db.query(query)


class EtcdWrapper:
    """
    Pack etcd library, send data to etcd
    """

    def __init__(self, conf):
        self.conf = conf
        self.host, self.port = self.conf['host'], self.conf['port']
        self.client = Client(host=self.host, port=self.port)
        self.test_connect()

    def test_connect(self):
        """
        test etcd server
        :return: 
        """
        url = 'http://' + self.host + ':' + str(self.port) + '/version'
        try:
            data = requests.get(url)
            if data.status_code == 200:
                log.info("etcd client init ok!")
                return True
            else:
                return False
        except Exception as e:
            log.error("\n%s", e)
            log.info("Check etcd server or network")
            return False

    def write(self, key, value):
        try:
            self.client.write(key, value)
        except Exception as e:
            log.error("\n%s", e)

    def read(self, key, value):
        try:
            return self.client.read(key)
        except Exception as e:
            log.error("\n%s", e)

    def delete(self, key):
        try:
            return self.client.delete(key)
        except Exception as e:
            log.error("\n%s", e)
