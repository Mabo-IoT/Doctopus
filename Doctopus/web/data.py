# -*- coding: utf-8 -*-
import falcon
import time

from Doctopus.lib.redis_wrapper import RedisWrapper


class Status:

    def  __init__(self, conf):
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
        :param req: 
        :param resp: 
        :return: 
        """
        check_data = self.__redis.smembers(self.set_name)
        if check_data:
            resp.body = check_data.pop()

        else:
            self.__put_order(self.order_name)
            data = self.__listen()

            if data:
                data = data.pop()
                resp.body = data
            else:
                resp.body = "None data"

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
            data = self.__redis.smembers(self.order_name)
            if  data or time.time() - start_time > timeout:
                break
                time.sleep(1)
        print('break')

        return data
