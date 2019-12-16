###############################################################
# pytest -v --capture=no tests/test_storage.py
# pytest -v  tests/test_storage.py
# pytest -v --capture=no tests/test_storage.py::TestStorage::<METHODNAME>
###############################################################
import os
from pathlib import Path
from pprint import pprint

import pytest
from cloudmesh.common.StopWatch import StopWatch
from cloudmesh.common.parameter import Parameter
from cloudmesh.common.util import HEADING, banner
from cloudmesh.common.util import path_expand
from cloudmesh.common.util import writefile
from cloudmesh.common.variables import Variables
from cloudmesh.common3.Benchmark import Benchmark
from cloudmesh.configuration.Config import Config
from cloudmesh.storage.Provider import Provider
from cloudmesh.common.debug import VERBOSE
from cloudmesh_installer.install.test import run as runcommand

Benchmark.debug()

user = Config()["cloudmesh.profile.user"]
variables = Variables()
VERBOSE(variables.dict())

key = variables['key']

cloud = variables.parameter('storage')

print(f"Test run for {cloud}")

if cloud is None:
    raise ValueError("storage is not set")

provider = None
config = None
bucket = None


def run(cmd):
    StopWatch.start(cmd)
    result = runcommand(cmd)
    StopWatch.stop(cmd)
    print(result)
    return result


@pytest.mark.incremental
class TestStorage(object):
    '''
    def test_credential_generation(self):
        """
        google yaml write [FILE_JSON] [--name=NAME]
        google yaml list [--name=NAME]
        google yaml add [FILE_JSON] [--name=NAME]
        google list storage
        :return:
        """

        banner("add json to yaml")
        cmd = "cms google yaml add ~/.cloudmesh/gcp.json"
        result = run(cmd)

        banner("write from yaml to json")

        cmd = "cms google yaml write  ~/.cloudmesh/google.json"
        result = run(cmd)

        banner("compare the original with the writtenn")

        cmd = "diff ~/.cloudmesh/gcp.json ~/.cloudmesh/google.json"
        result = run(cmd)

        assert result == ""
    '''
    def test_setup_provider(self):

        global provider
        global config
        global bucket

        provider = Provider(service=cloud)
        assert provider.kind == "google"
        config = Config()
        bucket=config[f'cloudmesh.storage.{cloud}.default.directory']

    # def test_list(self):
    #     HEADING()
    #     src = '/'
    #     StopWatch.start("list")
    #     contents = provider.list(src)
    #     StopWatch.stop("list")
    #     for c in contents:
    #         pprint(c)
    #
    #     assert len(contents) > 0



    def create_local_file(self, location, content):
        d = Path(os.path.dirname(path_expand(location)))
        print()
        print("TESTDIR:", d)

        d.mkdir(parents=True, exist_ok=True)

        writefile(path_expand(location), content)

    # def test_create_local_source(self):
    #     HEADING()
    #     StopWatch.start("create source")
    #     self.sourcedir = path_expand("~/.cloudmesh/storage/test/")
    #     self.create_local_file("~/.cloudmesh/storage/test/a/a.txt", "content of a")
    #     self.create_local_file("~/.cloudmesh/storage/test/a/b/b.txt", "content of b")
    #     self.create_local_file("~/.cloudmesh/storage/test/a/b/c/c.txt",
    #                            "content of c")
    #     StopWatch.stop("create source")
    #
    #     # test if the files are ok
    #     assert True

    def test_put(self):
        HEADING()

        # root="~/.cloudmesh"
        # src = "storage/test/a/a.txt"

        # src = f"local:{src}"
        # dst = f"aws:{src}"
        # test_file = self.p.put(src, dst)

        # src = "storage_a:test/a/a.txt"

        src = path_expand("~/.cloudmesh/storage/test/google_test/atest.txt")
        dst = "a169/a17/atest.txt"
        StopWatch.start("put")
        test_file = provider.put(src, dst)
        StopWatch.stop("put")

        pprint(test_file)

        assert test_file is not None

    # def test_put_recursive(self):
    #     HEADING()
    #
    #     # root="~/.cloudmesh"
    #     # src = "storage/test/a/a.txt"
    #
    #     # source = f"local:{src}"
    #     # destination = f"aws:{src}"
    #     # test_file = self.p.put(src, dst)
    #
    #     # src = "storage_a:test/a/a.txt"
    #
    #     src = "~/.cloudmesh/storage/test/"
    #     dst = '/'
    #     StopWatch.start("put")
    #     test_file = provider.put(src, dst, True)
    #     StopWatch.stop("put")
    #
    #     pprint(test_file)
    #
    #     assert test_file is not None

    def test_get(self):
        HEADING()
        src = "test"
        dst = "~/.cloudmesh/storage/test/google_test"
        StopWatch.start("get")
        file = provider.get(src, dst)
        StopWatch.stop("get")
        pprint(file)

        assert file is not None

    def test_list_all(self):
        HEADING()
        src = ''
        StopWatch.start("list")
        contents = provider.list(src)
        StopWatch.stop("list")

    def test_list_blob(self):
        HEADING()
        src = 'a10'
        StopWatch.start("list")
        contents = provider.list(src)
        StopWatch.stop("list")
        # for c in contents:
        #     pprint(c)
        #
        # assert len(contents) > 0
    #
    # def test_list_dir_only(self):
    #     HEADING()
    #     src = '/'
    #     dir = "a"
    #     StopWatch.start("list")
    #     contents = provider.list(src, dir, True)
    #     StopWatch.stop("list")
    #     for c in contents:
    #         pprint(c)
    #
    #     assert len(contents) > 0
    #
    # def test_search(self):
    #     HEADING()
    #     src = '/'
    #     filename = "a.txt"
    #     StopWatch.start("search")
    #     search_files = provider.search(src, filename, True)
    #     StopWatch.stop("search")
    #     pprint(search_files)
    #     assert len(search_files) > 0
    #     # assert filename in search_files[0]['cm']["name"]
    #
    def test_create_dir(self):
        HEADING()
        src = 'created_dir01/'
        StopWatch.start("create dir")
        directory = provider.create_dir(src)
        StopWatch.stop("create dir")

        pprint(directory)

        assert directory is not None
    #
    def test_delete(self):
        HEADING()
        src = 'top_folder5/sub_folder7/'
        StopWatch.start("delete")
        provider.delete(src)
        StopWatch.stop("delete")

    def test_blob_metadata(self):
        HEADING()
        blob_name = 'a10/atest.txt'
        StopWatch.start("test_blob_metadata")
        provider.blob_metadata(blob_name)
        StopWatch.stop("test_blob_metadata")

    # blob_metadata(f'{bucket_name}', 'a10/atest.txt')


    #
    def test_benchmark(self):
        Benchmark.print(sysinfo=False, csv=True, tag=cloud)
