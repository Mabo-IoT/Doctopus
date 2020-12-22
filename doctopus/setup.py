#!/usr/bin/env python
# coding=utf-8
'''
Author: Zhang Hengye
Date: 2020-04-20 17:03:55
LastEditors: Zhang Hengye
LastEditTime: 2020-12-22 10:28:25
'''

import sys

from setuptools import find_packages, setup

from Doctopus.version import version_

if sys.version_info[0] == 3 and sys.version_info[1] >= 5:
    setup(name='Doctopus',
          version=version_,
          author='',
          author_email='',
          description='A distributed data collector',
          packages=find_packages(exclude=[]),
          include_package_data=True,
          license='MIT',
          install_requires=[
              'pendulum', 'redis', 'influxdb', 'msgpack-python', 'toml',
              'falcon', 'waitress', 'kafka-python', 'paho-mqtt'
          ],
          entry_points={'console_scripts': ['doctopus=Doctopus:main']})
else:
    setup(name='Doctopus',
          version=version_,
          author='',
          author_email='',
          description='A distributed data collector',
          packages=find_packages(exclude=[]),
          include_package_data=True,
          license='MIT',
          install_requires=[
              'pendulum', 'redis', 'gevent', 'greenlet', 'influxdb',
              'msgpack-python', 'toml', 'falcon', 'waitress', 'kafka-python',
              'paho-mqtt'
          ],
          entry_points={'console_scripts': ['doctopus=Doctopus:main']})
