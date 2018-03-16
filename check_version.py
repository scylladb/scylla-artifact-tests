#!/usr/bin/env python

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2018 ScyllaDB

import logging
from avocado import Test


class CheckVersionDB(object):
    def __init__(self, host, user, passwd):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db_name = 'housekeeping'
        self.connect()
        self.log = logging.getLogger('check_version_db')

    def connect(self):
        import MySQLdb
        self.db = MySQLdb.connect(host=self.host,
                                  user=self.user,
                                  passwd=self.passwd,
                                  db=self.db_name)
        self.cursor = self.db.cursor()

    def close(self):
        self.db.close()

    def reconnect(self):
        self.close()
        self.connect()

    def commit(self):
        self.db.commit()

    def execute(self, sql, verbose=True):
        self.log.debug('SQL: {}'.format(sql))
        self.cursor.execute(sql)
        ret = self.cursor.fetchall()
        self.log.debug('RET: {}'.format(ret))
        return ret

    def get_last_id(self, uuid, repoid, version=None, table='housekeeping.repo', add_filter=None):
        # get last id of test uuid
        last_id = 0
        sql = 'select * from {} where uuid="{}" and repoid="{}"'.format(table, uuid, repoid)
        if version:
            sql += ' and version="{}" '.format(version)
        if add_filter:
            sql += ' {}'.format(add_filter)
        sql += ' order by -dt limit 1'

        ret = self.execute(sql)
        if len(ret) > 0:
            last_id = ret[0][0]
        return last_id

    def check_new_record(self, uuid, repoid, version=None, last_id=0, table='housekeeping.repo', add_filter=None):
        # verify download repo of test uuid is collected to repo table
        self.commit()
        sql = 'select * from {} where uuid="{}" and repoid="{}"'.format(table, uuid, repoid)
        if version:
            sql += ' and version="{}" '.format(version)
        if add_filter:
            sql += ' {}'.format(add_filter)
        sql += ' and id > {}'.format(last_id)
        ret = self.execute(sql)
        return len(ret) > 0


class EmptyTest(Test):
    """
    Workaround:
      We want Avocado to copy this module to VM, it will be used by scylla-artifacts.py
      But Avocado will raise error if the module doesn't contain valid subtest.
      So we add this empty test.

    :avocado: enable
    """
    def test_empty(self):
        pass
