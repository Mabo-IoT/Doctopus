#!/usr/bin/env python
# coding=utf-8
'''
Author: Zhang Hengye
Date: 2020-04-20 17:03:55
LastEditors: Zhang Hengye
LastEditTime: 2020-04-28 16:51:37
'''
# coding: utf-8

from setuptools import setup, find_packages
import sys

version = '0.4.7'

if sys.version_info[0] == 3 and sys.version_info[1] >= 5:
    setup(
        name='Doctopus',
        version=version,
        author='',
        author_email='',
        description='A distributed data collector',
        packages=find_packages(exclude=[]),
        include_package_data=True,
        license='MIT',
        install_requires=[
            'pendulum', 'redis', 'influxdb', 'msgpack-python',
            'toml', 'falcon', 'waitress', 'kafka-python',
            'paho-mqtt', 'psycopg2', 'dbutils'
        ],
        entry_points={
            'console_scripts': [
                'doctopus=Doctopus:main'
            ]
        }
    )
else:
    setup(
        name='Doctopus',
        version=version,
        author='',
        author_email='',
        description='A distributed data collector',
        packages=find_packages(exclude=[]),
        include_package_data=True,
        license='MIT',
        install_requires=[
            'pendulum', 'redis', 'gevent', 'greenlet',
            'influxdb', 'msgpack-python', 'toml', 'falcon',
            'waitress', 'kafka-python',
            'paho-mqtt', 'psycopg2', 'dbutils'
        ],
        entry_points={
            'console_scripts': [
                'doctopus=Doctopus:main'
            ]
        }
    )
