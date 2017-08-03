# -*- coding: utf-8 -*-
import falcon, os, waitress
from web.data import Status
from utils.util import get_conf


conf = get_conf('../conf/conf.toml')


def create_app(conf):
    api = falcon.API()
    api.add_route('/status/', Status(conf))
    return api

def get_app():
    return create_app(conf)


waitress.serve(get_app(), host='localhost', port=8000)


