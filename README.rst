Scylla Artifact Tests
=====================

Here you can find some avocado [1] tests for scylla artifacts. By artifacts,
we mean linux distribution binary packages, such as .deb and .rpm packages,
as well as the generated scylla AMIs [2].

Basic testing procedure
-----------------------

1) If we're testing distro packages, download and install them
2) Verify that the scylla service is running
3) Run nodetool status
4) Run cassandra-stress

What's inside?
--------------

1. A test file, scylla-artifacts.py
2. A scylla-artifacts.py.data dir, containing a multiplexer file for
   the test [3] (basically a way to pass parameters to the test.

Environment Setup
-----------------

1. Install avocado from pip
```
sudo yum install libvirt libvirt-devel pkgconfig gdb-gdbserver git -y
sudo pip install -r https://raw.githubusercontent.com/avocado-framework/avocado/36lts/requirements.txt
sudo pip install avocado-framework==36.* --upgrade
```
2. You can also reveference avocado doc [4] to install in by other methods

* [1] http://avocado-framework.github.io/
* [2] http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AMIs.html
* [3] http://avocado-framework.readthedocs.org/en/latest/MultiplexConfig.html
* [4] http://avocado-framework.readthedocs.io/en/48.0/GetStartedGuide.html#installing-avocado
