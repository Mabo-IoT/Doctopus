# -*- coding: utf-8 -*-
import falcon
import time
import requests
import json

from Doctopus.lib.database_wrapper import RedisWrapper


class Status:
    def __init__(self, conf):
        redis_conf = conf['redis']
        web_conf = conf['web']
        self.__redis = RedisWrapper(redis_conf)
        self.set_name = web_conf.get('set_name', 'status')
        self.order_name = web_conf.get('order_status', 'get_status')

    def on_get(self, req, resp):
        """
        1.check redis if there has set we need;
        2.if doesn't put get_status order in redis;
        3.listen to redis and take set
        4.if the parameter flush, refresh the history cache
        :param req: 
        :param resp: 
        :return: 
        """
        if req.params.get("flush"):
            self.__redis.delete(self.set_name)

        check_data = self.__redis.smembers(self.set_name)
        if check_data:
            resp.body = check_data.pop()

        else:
            self.__put_order(self.order_name)
            data = self.__listen()

            if data:
                data = data.pop().decode("utf-8")
                resp.body = data
            else:
                resp.body = json.dumps({'data': "No data"})

        resp.content_type = 'application/json'
        resp.status = falcon.HTTP_200

    def on_post(self):
        pass

    def __put_order(self, order_name):
        """
        put get_status order in redis
        :param order_name: status order_name
        :return: 
        """
        self.__redis.rpush("order_name", order_name)

    def __listen(self, timeout=3):
        """
        check redis specific set_name and return value if it is not None
        :param timeout: 
        :return: 
        """
        start_time = time.time()
        while True:
            data = self.__redis.smembers(self.set_name)
            if data or time.time() - start_time > timeout:
                break
            time.sleep(0.25)

        return data


class Restart:
    def __init__(self, conf):
        self.__redis = RedisWrapper(conf["redis"])

    def on_get(self, req, resp):
        self.__redis.rpush("order_name", "restart")
        resp.body = json.dumps("Restart now, wait please")
        resp.content_type = "application/json"
        resp.status = falcon.HTTP_200

    def on_post(self, req, resp):
        pass


class Reload:
    def __init__(self, conf):
        self.__redis = RedisWrapper(conf["redis"])

    def on_get(self, req, resp):
        self.__redis.rpush("order_name", "reload")
        resp.body = json.dumps("Reload now, wait please")
        resp.content_type = "application/json"
        resp.status = falcon.HTTP_200

    def on_post(self, req, resp):
        pass


class SeverStatus:
    def __init__(self, conf):
        self.__redis = RedisWrapper(conf["redis"])

    def on_get(self, req, resp):
        data = self.__redis.hgetall("node_data")
        resp.body = json.dumps(data if data else {"data": "No data"})
        resp.content_type = "application/json"
        resp.status = falcon.HTTP_200


class NodeStatus:
    def __init__(self, conf):
        self.__redis = RedisWrapper(conf["redis"])

    def on_get(self, req, resp, node):
        resp.body = self.get_data(node)
        resp.content_type = "application/json"
        resp.status = falcon.HTTP_200

    def get_data(self, node):
        """
        Gets the status information of the specified device
        :param node: Device code
        :return: json data
        """
        data = self.__redis.hgetall("node_data")
        if data.get(node):
            url = "http://{}/status".format(data.get(node)['ip'])
            return requests.get(url).text
        else:
            return json.dumps({'data': "No data"})


class Upload:
    def __init__(self, conf):
        self.__redis = RedisWrapper(conf["redis"])

    def on_get(self, req, resp):
        self.__redis.rpush("order_name", "upload")
        resp.body = json.dumps("Upload configuration now, wait please")
        resp.content_type = "application/json"
        resp.status = falcon.HTTP_200

