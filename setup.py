# coding: utf-8

from setuptools import setup, find_packages

setup(
    name='Doctopus',
    version='0.2.0',
    author='',
    author_email='',
    description='A distributed data collector',
    packages=find_packages(exclude=[]),
    include_package_data=True,
    license='MIT',
    install_requires=['pendulum', 'redis',
                      'influxdb', 'msgpack-python', 'toml', 'python-etcd', 'falcon'],
    entry_points={
        'console_scripts': [
            'doctopus=Doctopus:main'
        ]
    }
)
