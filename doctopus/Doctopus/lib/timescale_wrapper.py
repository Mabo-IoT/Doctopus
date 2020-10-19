#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""向TimescaleDB插入/查询数据"""

import logging
import time
from datetime import datetime

import psycopg2
import toml
from DBUtils.PooledDB import PooledDB
from psycopg2.errors import (DuplicateSchema, DuplicateTable, InterfaceError,
                             OperationalError)

log = logging.getLogger(__name__)


class TimescaleWrapper(object):
    """Communicate with TimescaleDB"""
    def __init__(self, conf):
        """Initialization

        :conf: Configuration info

        """
        self._conf: dict = conf
        # TimescaleDB configuration info
        self._host: str = conf.get('host', '127.0.0.1')
        self._port: int = conf.get('port', 5432)
        self._user: str = conf.get('user', None)
        self._password: str = conf.get('password', None)
        self._dbname: str = conf.get('dbname', None)
        self._schema_name: str = conf['table'].get('schema_name',
                                                   'device_tables')
        self._hypertable_name: str = conf['table'].get('hypertable_name',
                                                       'device_XX_XX')
        self._time_field: str = conf['table'].get('time_field', 'recordtime')

        # Connection pool
        self.conn = None
        self._getConnection()

    def __createPool(self):
        """Create TimescaleDB connection pool
        :returns: Connection pool

        """
        if None not in [self._user, self._password, self._dbname]:
            pool = PooledDB(
                # DBUtils parameters
                creator=psycopg2,
                mincached=5,
                maxcached=10,
                maxconnections=10,
                blocking=True,
                maxusage=0,
                ping=2,
                # psycopg2 parameters
                host=self._host,
                port=self._port,
                user=self._user,
                password=self._password,
                dbname=self._dbname)
        else:
            log.error("username/password/dbname cannot be None")

        return pool

    def _getConnection(self):
        """Get TimescaleDB connection
        :returns: connection object

        """
        while True:
            try:
                pool_obj = self.__createPool()
                self.conn = pool_obj.connection()
                break
            except Exception as err:
                log.error(
                    "TimescaleDB Connection error: {error}".format(error=err))
                time.sleep(2)

    def _createSchema(self, schema):
        """Create schema

        :schema: schema name to be created

        """
        SQL = "CREATE SCHEMA {schema_name};".format(schema_name=schema)
        try:
            cursor = self.conn.cursor()
            cursor.execute(SQL)
            self.conn.commit()
        except DuplicateSchema as warn:
            log.warning('{hint}: {warn_info}'.format(hint='Pass',
                                                     warn_info=warn))
        except Exception as err:
            log.error(err)

    def _createTable(self, schema, table, cols):
        """Create table

        :schema: schema name
        :table: table name to be created
        :cols: column name & data type:
                 {
                     'table1': 'int',
                     'table2': 'float',
                     'table3': 'str'
                 }

        """
        # Build SQL statements
        col_field = "id SERIAL PRIMARY KEY"
        for col, type_ in cols.items():
            if type_ in ['int', 'float']:
                col_field = '{curr_col}, {new_col} {attr_1} {attr_2}'.format(
                    curr_col=col_field,
                    new_col=col,
                    attr_1='DOUBLE PRECISION',
                    attr_2='NULL')
            else:
                col_field = '{curr_col}, {new_col} {attr_1} {attr_2}'.format(
                    curr_col=col_field,
                    new_col=col,
                    attr_1='VARCHAR',
                    attr_2='NULL')
        SQL = "CREATE TABLE {schema_name}.{table_name} ({columns});".format(
            schema_name=schema, table_name=table, columns=col_field)
        # Execute SQL statements
        try:
            cursor = self.conn.cursor()
            cursor.execute(SQL)
            self.conn.commit()
        except DuplicateTable as warn:
            log.warning('{hint}: {warn_info}'.format(hint='Pass',
                                                     warn_info=warn))
        except Exception as err:
            log.error(err)

    def _createHypertable(self, schema, hypertable, cols):
        """Create Hypertable

        :schema: schema name
        :hypertable: hypertable name to be created
        :cols: column name & data type:
                 {
                     'table1': 'int',
                     'table2': 'float',
                     'table3': 'str'
                 }

        """
        time_field = 'recordtime'
        # Build SQL statements
        col_field = "{time_field_name} TIMESTAMPTZ NOT NULL".format(
            time_field_name=time_field)
        for col, type_ in cols.items():
            if type_ in ['int', 'float']:
                col_field = '{curr_col}, {new_col} {attr_1} {attr_2}'.format(
                    curr_col=col_field,
                    new_col=col,
                    attr_1='DOUBLE PRECISION',
                    attr_2='NULL')
            else:
                col_field = '{curr_col}, {new_col} {attr_1} {attr_2}'.format(
                    curr_col=col_field,
                    new_col=col,
                    attr_1='VARCHAR',
                    attr_2='NULL')
        SQL = "CREATE TABLE {schema_name}.{table_name} ({columns});".format(
            schema_name=schema, table_name=hypertable, columns=col_field)
        SQL_HYPERTABLE = ("SELECT create_hypertable("
                          "'{schema_name}.{table_name}', "
                          "'{time_field_name}');").format(
                              schema_name=schema,
                              table_name=hypertable,
                              time_field_name=time_field)
        # Execute SQL statements
        try:
            cursor = self.conn.cursor()
            cursor.execute(SQL)
            cursor.execute(SQL_HYPERTABLE)
            self.conn.commit()
        except DuplicateTable as warn:
            log.warning('{hint}: {warn_info}'.format(hint='Pass',
                                                     warn_info=warn))
        except Exception as err:
            log.error(err)

    def _insertData(self, schema, table, datas):
        """Insert data into the table

        :schema: schema name
        :table: table name
        :datas: data to be inserted:
            datas = {
                'XXX': {
                    'name': 'XXX',
                    'title': 'XXX',
                    'value': 100,
                    'unit': 'km/h'
                },
            }

        """
        col_field = ''
        values = ''
        dtime = datetime.now()
        # Build SQL statements
        for col, data in datas.items():
            if not str(data['value']):
                # 该值为空
                continue
            else:
                col_field = '{curr_col}, {new_col}'.format(
                    curr_col=col_field, new_col=col).strip(',')
                values = '{curr_value}, {new_value}'.format(
                    curr_value=values, new_value=data['value']).strip(',')
        SQL = ("INSERT INTO {schema_name}.{table_name} "
               "({time_field_name}, {columns}) "
               "VALUES (%s, {values})").format(
                   schema_name=schema,
                   table_name=table,
                   time_field_name=self._time_field,
                   columns=col_field,
                   values=values)
        # Execute SQL statements
        try:
            cursor = self.conn.cursor()
            cursor.execute(SQL, (dtime, ))
            self.conn.commit()
        except DuplicateTable as warn:
            log.warning('{hint}: {warn_info}'.format(hint='Pass',
                                                     warn_info=warn))
        except Exception as err:
            log.error(err)

    def _queryData(self, schema, table, order='id', limit=5):
        """Query data from the table

        :schema: schema name
        :table: table name
        :order: ORDER BY order
        :limit: LIMIT limit
        :result SELECT result

        """
        result = list()
        # Build SQL statements
        SQL = ("SELECT * FROM {schema_name}.{table_name} "
               "ORDER BY {order} DESC LIMIT {limit}").format(
                   schema_name=schema,
                   table_name=table,
                   order=order,
                   limit=limit)
        # Execute SQL statements
        try:
            cursor = self.conn.cursor()
            cursor.execute(SQL)
            result = cursor.fetchall()
        except DuplicateTable as warn:
            log.warning('{hint}: {warn_info}'.format(hint='Pass',
                                                     warn_info=warn))
        except Exception as err:
            log.error(err)

        return result

    def use4test(self):
        """Use for test"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('show timezone;')
            data = cursor.fetchall()
            print(data)
        except (OperationalError, InterfaceError):
            log.error('Recreating connection pool......')
            self._getConnection()
        except Exception as err:
            log.error(err)


if __name__ == "__main__":
    confile = '../conf/chitu_conf.toml'
    conf = toml.load(confile)['timescale']
    client = TimescaleWrapper(conf)

    # Test _createSchema
    schema = 'test'
    client._createSchema(schema=schema)

    # Test _createTable
    table = 'num1'
    cols = {'x': 'int', 'y': 'str', 'z': 'float', 'a': 'int'}
    client._createTable(schema=schema, table=table, cols=cols)

    # Test _createHypertable
    hypertable = 'hyper_table'
    client._createHypertable(schema=schema, hypertable=hypertable, cols=cols)

    # Test _insertData
    datas = {
        'x': {
            'name': 'x',
            'title': '速度',
            'value': 100,
            'unit': 'km/h'
        },
        'y': {
            'name': 'y',
            'title': '牵引力',
            'value': 200,
            'unit': 'N'
        },
        'z': {
            'name': 'z',
            'title': '功率',
            'value': 300,
            'unit': 'Kw'
        },
        'a': {
            'name': 'a',
            'title': '加速度',
            'value': 400,
            'unit': 'm/s^2'
        },
    }
    client._insertData(schema=schema, table=hypertable, datas=datas)

    # Test _insertData
    result = client._queryData(schema=schema,
                               table=hypertable,
                               order='recordtime',
                               limit=2)
    print(result)

    # Test use4test
    while 1:
        client.use4test()
        time.sleep(1)
