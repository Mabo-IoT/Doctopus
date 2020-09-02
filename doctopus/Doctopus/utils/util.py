# -*- coding: utf-8 -*-
from sys import version_info

import toml


def get_conf(conf_file_path=r'conf/conf.toml'):
    """read toml conf file for latter use.


    :param conf_file_path: absolute path of conf file.
    :return:a dict contains configured infomation.
    """
    if version_info[0] == 3:
        with open(conf_file_path, encoding='utf-8') as conf_file:
            config = toml.loads(conf_file.read())
    else:
        with open(conf_file_path) as conf_file:
            config = toml.loads(conf_file.read())

    return config
