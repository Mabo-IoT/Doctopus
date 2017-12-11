# -*- coding: utf-8 -*-

import requests
import argparse

from Doctopus.utils.util import get_conf

def chitu(command):
    url = "http://127.0.0.1:8001/" + command
    requests.get(url)

def ziyan(command):
    url = "http://127.0.0.1:8000/" + command
    requests.get(url)

if __name__ == "__main__":
    parse = argparse.ArgumentParser()
    parse.add_argument('action', action='store')
    command = parse.parse_args().action
    conf = get_conf("../conf/conf.toml")
    fn = {"ziyan": ziyan, "chitu": chitu}
    fn.get(conf["application"])(command)
