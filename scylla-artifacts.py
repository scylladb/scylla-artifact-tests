#!/usr/bin/python

import os
import logging
import threading

from avocado import Test
from avocado import main
from avocado.utils import distro
from avocado.utils import software_manager
from avocado.utils import process
from avocado.utils import path
from avocado.utils import service
from avocado.utils import wait
from avocado.utils import network


class StartServiceError(Exception):
    pass


class StopServiceError(Exception):
    pass


class RestartServiceError(Exception):
    pass


class InstallPackageError(Exception):
    pass


def get_scylla_logs():
    try:
        journalctl_cmd = path.find_command('journalctl')
        process.run('sudo %s -f '
                    '-u scylla-io-setup.service '
                    '-u scylla-server.service '
                    '-u scylla-jmx.service' % journalctl_cmd,
                    verbose=True, ignore_status=True)
    except path.CmdNotFoundError:
        process.run('tail -f /var/log/syslog | grep scylla', ignore_status=True)


class ScyllaServiceManager(object):

    def __init__(self):
        self.services = ['scylla-server', 'scylla-jmx']

    def _scylla_service_is_up(self):
        srv_manager = service.ServiceManager()
        for srv in self.services:
            srv_manager.status(srv)
        return not network.is_port_free(9042, 'localhost')

    def wait_services_up(self):
        service_start_timeout = 900
        output = wait.wait_for(func=self._scylla_service_is_up,
                               timeout=service_start_timeout, step=5)
        if output is None:
            e_msg = 'Scylla service does not appear to be up after %s s' % service_start_timeout
            raise StartServiceError(e_msg)

    def start_services(self):
        srv_manager = service.ServiceManager()
        for srv in self.services:
            srv_manager.start(srv)
        for srv in self.services:
            if not srv_manager.status(srv):
                if service.get_name_of_init() == 'systemd':
                    process.run('journalctl -xe', ignore_status=True, verbose=True)
                e_msg = ('Failed to start service %s '
                         '(see logs for details)' % srv)
                raise StartServiceError(e_msg)

    def stop_services(self):
        srv_manager = service.ServiceManager()
        for srv in reversed(self.services):
            srv_manager.stop(srv)
        for srv in self.services:
            if srv_manager.status(srv):
                if service.get_name_of_init() == 'systemd':
                    process.run('journalctl -xe', ignore_status=True, verbose=True)
                e_msg = ('Failed to stop service %s '
                         '(see logs for details)' % srv)
                raise StopServiceError(e_msg)

    def restart_services(self):
        srv_manager = service.ServiceManager()
        for srv in self.services:
            srv_manager.restart(srv)
        for srv in self.services:
            if not srv_manager.status(srv):
                if service.get_name_of_init() == 'systemd':
                    process.run('journalctl -xe', ignore_status=True, verbose=True)
                e_msg = ('Failed to restart service %s '
                         '(see logs for details)' % srv)
                raise RestartServiceError(e_msg)


class ScyllaInstallGeneric(object):

    def __init__(self, sw_repo=None, mode='ci'):
        self.base_url = 'https://s3.amazonaws.com/downloads.scylladb.com/'
        self.mode = mode
        self.sw_manager = software_manager.SoftwareManager()
        self.sw_repo_src = sw_repo
        self.sw_repo_dst = None
        self.log = logging.getLogger('avocado.test')
        self.srv_manager = ScyllaServiceManager()

    def run(self):
        self.sw_manager.upgrade()
        get_packages = getattr(self, 'setup_%s' % self.mode)
        pkgs = get_packages()
        for pkg in pkgs:
            if not self.sw_manager.install(pkg):
                e_msg = ('Package %s could not be installed '
                         '(see logs for details)' % os.path.basename(pkg))
                raise InstallPackageError(e_msg)
        process.run('/usr/lib/scylla/scylla_io_setup', shell=True)
        self.srv_manager.start_services()
        self.srv_manager.wait_services_up()


class ScyllaInstallUbuntu(ScyllaInstallGeneric):

    def __init__(self, sw_repo, mode='ci'):
        super(ScyllaInstallUbuntu, self).__init__(sw_repo, mode)
        self.sw_repo_dst = '/etc/apt/sources.list.d/scylla.list'


class ScyllaInstallUbuntu1404(ScyllaInstallUbuntu):

    def setup_ci(self):
        process.run('curl %s -o %s' % (self.sw_repo_src, self.sw_repo_dst),
                    shell=True, sudo=True)
        self.sw_manager.upgrade()
        return ['scylla-server', 'scylla-jmx', 'scylla-tools']

    def setup_release(self):
        repo_src_1_0 = 'http://downloads.scylladb.com/deb/ubuntu/scylla-1.0.list'
        repo_src_1_1 = 'http://downloads.scylladb.com/deb/ubuntu/scylla-1.1.list'
        repo_src_unstable = 'http://downloads.scylladb.com/deb/unstable/ubuntu/master/latest/scylla.list'
        repo_src = repo_src_1_1
        if self.mode == '1.0':
            repo_src = repo_src_1_0
        elif self.mode == '1.1':
            repo_src = repo_src_1_1
        elif self.mode == 'unstable':
            repo_src = repo_src_unstable
        process.run('curl %s -o %s' % (repo_src, self.sw_repo_dst),
                    shell=True, sudo=True)
        self.sw_manager.upgrade()
        return ['scylla-server', 'scylla-jmx', 'scylla-tools']


class ScyllaInstallUbuntu1604(ScyllaInstallUbuntu):

    def setup_ci(self):
        process.run('curl %s -o %s' % (self.sw_repo_src, self.sw_repo_dst),
                    shell=True, sudo=True)
        self.sw_manager.upgrade()
        return ['scylla-server', 'scylla-jmx', 'scylla-tools']

    def setup_release(self, release_type='1.1'):
        raise NotImplementedError('Ubuntu 16.04 release packages coming up, '
                                  'hold on tight...')


class ScyllaInstallFedora(ScyllaInstallGeneric):

    def __init__(self, sw_repo, mode='ci'):
        super(ScyllaInstallFedora, self).__init__(sw_repo, mode)
        self.sw_repo_dst = '/etc/yum.repos.d/scylla.repo'


class ScyllaInstallFedora22(ScyllaInstallFedora):

    def setup_ci(self):
        process.run('curl %s -o %s' % (self.sw_repo_src, self.sw_repo_dst),
                    shell=True, sudo=True)
        self.sw_manager.upgrade()
        return ['scylla-server', 'scylla-jmx', 'scylla-tools']

    def setup_release(self):
        repo_src_1_0 = 'http://downloads.scylladb.com/rpm/fedora/scylla-1.0.repo'
        repo_src_1_1 = 'http://downloads.scylladb.com/rpm/fedora/scylla-1.1.repo'
        repo_src_unstable = 'http://downloads.scylladb.com/rpm/unstable/fedora/master/latest/scylla.repo'
        repo_src = repo_src_1_1
        if self.mode == '1.0':
            repo_src = repo_src_1_0
        elif self.mode == '1.1':
            repo_src = repo_src_1_1
        elif self.mode == 'unstable':
            repo_src = repo_src_unstable
        process.run('curl %s -o %s' % (repo_src, self.sw_repo_dst),
                    shell=True, sudo=True)
        self.sw_manager.upgrade()
        return ['scylla-server', 'scylla-jmx', 'scylla-tools']


class ScyllaInstallCentOS(ScyllaInstallGeneric):

    def __init__(self, sw_repo, mode='ci'):
        super(ScyllaInstallCentOS, self).__init__(sw_repo, mode)
        self.sw_repo_dst = '/etc/yum.repos.d/scylla.repo'

    def _centos_remove_system_packages(self):
        self.sw_manager.remove('boost-thread')
        self.sw_manager.remove('boost-system')
        self.sw_manager.remove('abrt')


class ScyllaInstallCentOS7(ScyllaInstallCentOS):

    def setup_ci(self):
        self._centos_remove_system_packages()
        process.run('curl %s -o %s' % (self.sw_repo_src, self.sw_repo_dst),
                    shell=True, sudo=True)
        self.sw_manager.upgrade()
        return ['scylla-server', 'scylla-jmx', 'scylla-tools']

    def setup_release(self):
        self._centos_remove_system_packages()
        repo_src_1_0 = 'http://downloads.scylladb.com/rpm/centos/scylla-1.0.repo'
        repo_src_1_1 = 'http://downloads.scylladb.com/rpm/centos/scylla-1.1.repo'
        repo_src_unstable = 'http://downloads.scylladb.com/rpm/unstable/centos/master/latest/scylla.repo'
        repo_src = repo_src_1_1
        if self.mode == '1.0':
            repo_src = repo_src_1_0
        elif self.mode == '1.1':
            repo_src = repo_src_1_1
        elif self.mode == 'unstable':
            repo_src = repo_src_unstable
        process.run('curl %s -o %s' % (repo_src, self.sw_repo_dst),
                    shell=True, sudo=True)
        self.sw_manager.upgrade()
        return ['scylla-server', 'scylla-jmx', 'scylla-tools']


class ScyllaInstallAMI(ScyllaInstallGeneric):

    def run(self):
        self.log.info("Testing AMI, let's just check if the DB is up...")
        self.srv_manager.wait_services_up()


class ScyllaArtifactSanity(Test):

    """
    Sanity check of the build artifacts (deb, rpm)

    setup: Install artifacts (deb, rpm)
    1) Run cassandra-stress
    2) Run nodetool
    """
    setup_done_file = None
    srv_manager = ScyllaServiceManager()

    def get_setup_file_done(self):
        tmpdir = os.path.dirname(self.workdir)
        return os.path.join(tmpdir, 'scylla-setup-done')

    def scylla_setup(self):
        # Let's start the logs thread before package install
        self._log_collection_thread = threading.Thread(target=get_scylla_logs)
        self._log_collection_thread.start()
        sw_repo = self.params.get('sw_repo', default=None)
        mode = self.params.get('mode', default='ci')
        ami = self.params.get('ami', default=False) is True
        if sw_repo is not None:
            if sw_repo.strip() != 'EMPTY':
                mode = 'ci'

        detected_distro = distro.detect()
        fedora_22 = (detected_distro.name.lower() == 'fedora' and
                     detected_distro.version == '22')
        ubuntu_14_04 = (detected_distro.name.lower() == 'ubuntu' and
                        detected_distro.version == '14' and
                        detected_distro.release == '04')
        ubuntu_16_04 = (detected_distro.name.lower() == 'ubuntu' and
                        detected_distro.version == '16' and
                        detected_distro.release == '04')
        centos_7 = (detected_distro.name.lower() == 'centos' and
                    detected_distro.version == '7')

        installer = None

        if ami:
            installer = ScyllaInstallAMI()

        elif ubuntu_14_04:
            installer = ScyllaInstallUbuntu1404(sw_repo=sw_repo, mode=mode)

        elif ubuntu_16_04:
            installer = ScyllaInstallUbuntu1604(sw_repo=sw_repo, mode=mode)

        elif fedora_22:
            installer = ScyllaInstallFedora22(sw_repo=sw_repo, mode=mode)

        elif centos_7:
            installer = ScyllaInstallCentOS7(sw_repo=sw_repo, mode=mode)

        else:
            self.skip('Unsupported OS: %s' % detected_distro)

        installer.run()
        os.mknod(self.get_setup_file_done())

    def setUp(self):
        if not os.path.isfile(self.get_setup_file_done()):
            self.scylla_setup()

    def run_cassandra_stress(self):
        def check_output(result):
            output = result.stdout + result.stderr
            lines = output.splitlines()
            for line in lines:
                if 'java.io.IOException' in line:
                    self.fail('cassandra-stress: %s' % line.strip())
        cassandra_stress_exec = path.find_command('cassandra-stress')
        stress_populate = ('%s write n=10000 -mode cql3 native -pop seq=1..10000' %
                           cassandra_stress_exec)
        result_populate = process.run(stress_populate)
        check_output(result_populate)
        stress_mixed = ('%s mixed duration=1m -mode cql3 native '
                        '-rate threads=10 -pop seq=1..10000' %
                        cassandra_stress_exec)
        result_mixed = process.run(stress_mixed, shell=True)
        check_output(result_mixed)

    def run_nodetool(self):
        nodetool_exec = path.find_command('nodetool')
        nodetool = '%s status' % nodetool_exec
        process.run(nodetool)

    def test_after_install(self):
        self.run_nodetool()
        self.run_cassandra_stress()

    def test_after_stop_start(self):
        self.srv_manager.stop_services()
        self.srv_manager.start_services()
        self.srv_manager.wait_services_up()
        self.run_nodetool()
        self.run_cassandra_stress()

    def test_after_restart(self):
        self.srv_manager.restart_services()
        self.srv_manager.wait_services_up()
        self.run_nodetool()
        self.run_cassandra_stress()

if __name__ == '__main__':
    main()
