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

* [1] http://avocado-framework.github.io/
* [2] http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AMIs.html
* [3] http://avocado-framework.readthedocs.org/en/latest/MultiplexConfig.html
