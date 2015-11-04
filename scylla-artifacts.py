#!/usr/bin/python

import os

from avocado import Test
from avocado import main
from avocado.utils import distro
from avocado.utils import software_manager
from avocado.utils import process
from avocado.utils import path
from avocado.utils import download
from avocado.utils import service


class ScyllaArtifactSanity(Test):

    """
    Sanity check of the build artifacts (deb, rpm)

    setup: Install artifacts (deb, rpm)
    1) Run cassandra-stress
    2) Run nodetool
    """
    setup_done_file = None

    def get_setup_file_done(self):
        tmpdir = os.path.dirname(self.workdir)
        return os.path.join(tmpdir, 'scylla-setup-done')

    def download_fedora_22_pkgs(self):
        x86_url = os.path.join(self.base_url, 'rpm', 'fedora', '22', 'x86_64')
        noarch_url = os.path.join(self.base_url, 'rpm', 'fedora', '22', 'noarch')
        scylla_server = 'scylla-server-0.11-20151102.f849c4b.fc22.x86_64.rpm'
        scylla_jmx = 'scylla-jmx-0.11-20151102.36ed2cf.fc22.noarch.rpm'
        scylla_tools = 'scylla-tools-0.11-20151102.ff63598.fc22.noarch.rpm'
        rpms = []

        rpms_download_info = [(x86_url, scylla_server),
                              (noarch_url, scylla_jmx),
                              (noarch_url, scylla_tools)]

        for b_url, rpm in rpms_download_info:
            src = os.path.join(b_url, rpm)
            dst = os.path.join(self.outputdir, rpm)
            rpms.append(download.get_file(src, dst))

        return rpms

    def download_ubuntu_14_04_pkgs(self):
        base_url = os.path.join(self.base_url, 'deb', 'ubuntu', 'dists',
                                'trusty', 'scylladb', 'multiverse',
                                'binary-amd64')
        scylla_server = 'scylla-server_0.11-ubuntu1_amd64.deb'
        scylla_jmx = 'scylla-jmx_0.11-ubuntu1_all.deb'
        scylla_tools = 'scylla-tools_0.11-ubuntu1_all.deb'
        thrift = 'libthrift0_0.9.1-ubuntu1_amd64.deb'
        debs = []

        debs_download_info = [(base_url, thrift),
                              (base_url, scylla_server),
                              (base_url, scylla_jmx),
                              (base_url, scylla_tools)]

        for b_url, deb in debs_download_info:
            src = os.path.join(b_url, deb)
            dst = os.path.join(self.outputdir, deb)
            debs.append(download.get_file(src, dst))

        return debs

    def scylla_setup(self):
        self.base_url = 'https://s3.amazonaws.com/downloads.scylladb.com/'

        detected_distro = distro.detect()
        fedora_22 = (detected_distro.name.lower() == 'fedora' and
                     detected_distro.version == '22')
        ubuntu_14_04 = (detected_distro.name.lower() == 'ubuntu' and
                        detected_distro.version == '14' and
                        detected_distro.release == '04')
        pkgs = []
        if fedora_22:
            pkgs = self.download_fedora_22_pkgs()
        elif ubuntu_14_04:
            pkgs = self.download_ubuntu_14_04_pkgs()
        else:
            self.skip('Unsupported OS: %s' % detected_distro)

        sw_manager = software_manager.SoftwareManager()
        for pkg in pkgs:
            if not sw_manager.install(pkg):
                raise self.error('Package %s could not be installed '
                                 '(see logs for details)' %
                                 os.path.basename(pkg))

        srv_manager = service.ServiceManager()
        services = ['scylla-server', 'scylla-jmx']
        for srv in services:
            srv_manager.start(srv)
        for srv in services:
            if not srv_manager.status(srv):
                self.error('Failed to start service %s '
                           '(see logs for details)' % srv)

        os.mknod(self.get_setup_file_done())

    def setUp(self):
        if not os.path.isfile(self.get_setup_file_done()):
            self.scylla_setup()

    def test_cassandra_stress(self):
        cassandra_stress_exec = path.find_command('cassandra-stress')
        cassandra_stress = '%s write -mode cql3 native' % cassandra_stress_exec
        process.run(cassandra_stress)

    def test_nodetool(self):
        nodetool_exec = path.find_command('nodetool')
        nodetool = '%s status' % nodetool_exec
        process.run(nodetool)


if __name__ == '__main__':
    main()

