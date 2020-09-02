# -*- coding: utf-8 -*-
import falcon

from Doctopus.web.data import Status, Reload, Restart, Upload, SeverStatus, NodeStatus

from Doctopus.utils.util import get_conf


def create_client(conf):
    api = falcon.API()
    api.add_route('/status/', Status(conf))
    api.add_route('/reload/', Reload(conf))
    api.add_route('/restart/', Restart(conf))
    api.add_route('/upload/', Upload(conf))
    return api


def create_server(conf):
    api = falcon.API()
    api.add_route('/status/', SeverStatus(conf))
    api.add_route('/status/{node}/', NodeStatus(conf))
    return api


def get_app(selection='client'):
    if selection == "client":
        return create_client(get_conf())
    elif selection == "server":
        return create_server(get_conf())
