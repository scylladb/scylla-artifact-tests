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
# Copyright (c) 2017 ScyllaDB

import logging
import tempfile
import json
import re
import MySQLdb

from avocado import Test
from avocado.utils import process
from avocado import main


class CheckVersionDB(object):
    def __init__(self, host, user, passwd):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db_name = 'housekeeping'
        self.connect()
        self.log = logging.getLogger('scylla_private_repo')

    def connect(self):
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


class PrivateRepo(object):
    def __init__(self, repo_url, pkginfo_url, redirect_url):
        self.repo_url = repo_url
        self.pkginfo_url = pkginfo_url
        self.redirect_url = redirect_url
        self.uuid = self._get_uuid()

    def _get_uuid(self):
        match = re.findall('https://repositories.scylladb.com/scylla/repo/([\w_-]*)/', self.repo_url)
        if len(match) == 1:
            return match[0]
        return None


class RHELPrivateRepo(PrivateRepo):
    def __init__(self, repo_url, pkginfo_url, redirect_url):
        super(RHELPrivateRepo, self).__init__(repo_url, pkginfo_url, redirect_url)
        self.body_prefix = ['[scylla', 'name=', 'baseurl=', 'enabled=', 'gpgcheck=']


class DebianPrivateRepo(PrivateRepo):
    def __init__(self, repo_url, pkginfo_url, redirect_url):
        super(DebianPrivateRepo, self).__init__(repo_url, pkginfo_url, redirect_url)
        self.body_prefix = ['deb']


class ScyllaPrivateRepoSanity(Test):
    """
    Useful repo can be got from private link.
    Verify redirection works.
    Verify download info can be collected to housekeeping db.

    :avocado: enable
    """
    def __init__(self, *args, **kwargs):
        super(ScyllaPrivateRepoSanity, self).__init__(*args, **kwargs)
        self.log = logging.getLogger('scylla_private_repo')

    def setUp(self):
        repo_url = self.params.get('repo_url')
        pkginfo_url = self.params.get('pkginfo_url')
        redirect_url = self.params.get('redirect_url')
        name = self.params.get('name', default='centos7')
        if name in ['centos7']:
            self.private_repo = RHELPrivateRepo(repo_url, pkginfo_url, redirect_url)
        elif name in ['ubuntu1404', 'ubuntu1604', 'debian8']:
            self.private_repo = DebianPrivateRepo(repo_url, pkginfo_url, redirect_url)
        self.cvdb = CheckVersionDB(self.params.get('host'),
                                   self.params.get('user'),
                                   self.params.get('passwd'))
        self.log.debug(self.cvdb.execute('show tables'))

    def tearDown(self):
        self.cvdb.close()

    def check_collect_info(self):
        pass

    def test_generate_repo(self):
        # get last id of test uuid
        last_id = 0
        ret = self.cvdb.execute('select * from housekeeping.repo where uuid="{}" order by -dt limit 1'.format(self.private_repo.uuid))
        if len(ret) > 0:
            last_id = ret[0][0]

        tmp = tempfile.mktemp(prefix='scylla_private_repo')
        process.run('curl {} -o {}'.format(self.private_repo.repo_url, tmp), verbose=True)
        with open(tmp, 'r') as f:
            repo_body = f.read()

        for line in repo_body.split('\n'):
            valid_prefix = False
            for prefix in self.private_repo.body_prefix:
                if line.startswith(prefix) or len(line.strip()) == 0:
                    valid_prefix = True
                    break
            self.log.debug(line)
            assert valid_prefix, 'repo content has invalid line: {}'.format(line)

        # verify download repo of test uuid is collected to repo table
        self.cvdb.commit()
        ret = self.cvdb.execute('select * from housekeeping.repo where uuid="{}" and id > {}'.format(self.private_repo.uuid, last_id))
        assert len(ret) > 0

    def test_redirect(self):
        # get last id of test uuid
        last_id = 0
        ret = self.cvdb.execute('select * from housekeeping.repodownload where uuid="{}" order by -dt limit 1'.format(self.private_repo.uuid))
        if len(ret) > 0:
            last_id = ret[0][0]

        tmp = tempfile.mktemp(prefix='scylla_private_repo')
        result = process.run('curl {} -o {}'.format(self.private_repo.pkginfo_url, tmp), verbose=True)
        print result
        with open(tmp, 'r') as f:
            tmp_content = f.read()
        response = json.loads(tmp_content)
        self.log.debug(response)
        assert response['errorMessage'] == u'HandlerDemo.ResponseFound Redirection: Resource found elsewhere'
        assert response['errorType'] == self.private_repo.redirect_url

        # verify download info of test uuid is collected to repodownload table
        self.cvdb.commit()
        ret = self.cvdb.execute('select * from housekeeping.repodownload where uuid="{}" and id > {}'.format(self.private_repo.uuid, last_id))
        assert len(ret) > 0


if __name__ == '__main__':
    main()
