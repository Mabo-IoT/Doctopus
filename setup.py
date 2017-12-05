# coding: utf-8

from setuptools import setup, find_packages
import sys

if sys.version_info[0] == 3 and sys.version_info[1] >= 5:
    setup(
        name='Doctopus',
        version='0.3.0',
        author='',
        author_email='',
        description='A distributed data collector',
        packages=find_packages(exclude=[]),
        include_package_data=True,
        license='MIT',
        install_requires=['pendulum', 'redis',
                          'influxdb', 'msgpack-python', 'toml', 'python-etcd', 'falcon', 'waitress'],
        entry_points={
            'console_scripts': [
                'doctopus=Doctopus:main'
            ]
        }
    )
else:
    setup(
        name='Doctopus',
        version='0.3.0',
        author='',
        author_email='',
        description='A distributed data collector',
        packages=find_packages(exclude=[]),
        include_package_data=True,
        license='MIT',
        install_requires=['pendulum', 'redis', 'gevent', 'greenlet',
                          'influxdb', 'msgpack-python', 'toml', 'python-etcd', 'falcon', 'waitress'],
        entry_points={
            'console_scripts': [
                'doctopus=Doctopus:main'
            ]
        }
    )
