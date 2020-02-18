###############################################################
# cms set cloud=google
# cms set storage=google
# pytest -v --capture=no tests/test_storage_google..py:::TestStorage::<METHODNAME>
# pytest -x -v --capture=no tests/test_storage_google.py
###############################################################
import os
from pathlib import Path
from pprint import pprint

import pytest
from cloudmesh.common.Benchmark import Benchmark
from cloudmesh.common.StopWatch import StopWatch
from cloudmesh.common.debug import VERBOSE
from cloudmesh.common.util import HEADING
from cloudmesh.common.util import path_expand
from cloudmesh.common.util import writefile
from cloudmesh.common.variables import Variables
from cloudmesh.configuration.Config import Config
from cloudmesh.storage.Provider import Provider
from cloudmesh_installer.install.installer import run as runcommand

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

# MUST remove quotes in order to test credentials
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

    def create_local_file(self, location, content):
        d = Path(os.path.dirname(path_expand(location)))
        print()
        print("TESTDIR:", d)

        d.mkdir(parents=True, exist_ok=True)

        writefile(path_expand(location), content)

    def test_create_local_source(self):
        HEADING()
        StopWatch.start("create source")
        self.sourcedir = path_expand("~/.cloudmesh/storage/test/")
        self.create_local_file("~/.cloudmesh/storage/test/a/a.txt", "content of a")
        self.create_local_file("~/.cloudmesh/storage/test/a/b/b.txt", "content of b")
        self.create_local_file("~/.cloudmesh/storage/test/a/b/c/c.txt",
                               "content of c")
        StopWatch.stop("create source")

        # test if the files are ok
        assert True

    def test_create_dir(self):
        HEADING()
        src = 'a/b/'
        StopWatch.start("create dir ")
        directory = provider.create_dir(src)
        StopWatch.stop("create dir ")

        pprint(directory)

        assert directory is not None

    def test_put(self):
        HEADING()
        src = path_expand("~/.cloudmesh/storage/test/a/a.txt")
        src = f"{src}"
        dst = 'a/a.txt'
        StopWatch.start("put")
        test_file = provider.put(src, dst)
        StopWatch.stop("put")
        pprint(test_file)

        assert test_file is not None


    def test_get(self):
        HEADING()

        src = "a"
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

    def test_list_blob_keyword(self):
        HEADING()
        src = 'a'
        StopWatch.start("list")
        contents = provider.list(src)
        StopWatch.stop("list")

    def test_delete(self):
        HEADING()
        src = 'top_folder5/sub_folder7/'
        provider.create_dir(src)

        StopWatch.start("delete")
        provider.delete(src)
        StopWatch.stop("delete")

    def test_blob_metadata(self):
        HEADING()
        from cloudmesh.google.storage.Provider import Provider
        provider = Provider(service=cloud)
        blob_name = 'a/a.txt'
        StopWatch.start("test_blob_metadata")
        provider.blob_metadata(blob_name)
        StopWatch.stop("test_blob_metadata")

    # blob_metadata(f'{bucket_name}', 'a10/atest.txt')

    def test_rename_blob(self):
        HEADING()
        from cloudmesh.google.storage.Provider import Provider
        provider = Provider(service=cloud)
        blob_name = 'top_folder11/sub_folder7/test2'
        provider.create_dir(blob_name)
        new_name = 'top_folder11/sub_folder7/test2_new'
        StopWatch.start("test_rename_blob")
        provider.rename_blob(blob_name,new_name)
        StopWatch.stop("test_rename_blob")

    # rename_blob(f'{bucket_name}', '{blob_name}', '{new_name}')

    def test_copy_blob_btw_buckets(self):
        HEADING()
        from cloudmesh.google.storage.Provider import Provider
        provider = Provider(service=cloud)
        blob_name = 'a/a.txt'
        bucket_name_dest = 'cloudmesh_gcp2'
        blob_name_dest = 'a/a.txt'
        StopWatch.start("test_copy_blob_btw_buckets")
        provider.copy_blob_btw_buckets(blob_name, bucket_name_dest, blob_name_dest)
        StopWatch.stop("test_copy_blob_btw_buckets")

    # copy_blob(f'{bucket_name}', 'download_file1', 'my-new-bucket_shre', 'a1692_new/a18_new')

    def test_create_bucket(self):
        HEADING()
        from cloudmesh.google.storage.Provider import Provider
        provider = Provider(service=cloud)
        new_bucket_name = 'cloudmesh_gcp2'
        StopWatch.start("test_create_bucket_google")
        provider.create_bucket(new_bucket_name)
        StopWatch.stop("test_create_bucket_google")

    def test_list_bucket(self):
        HEADING()
        from cloudmesh.google.storage.Provider import Provider
        provider = Provider(service=cloud)
        StopWatch.start("test_list_bucket_google")
        provider.list_bucket()
        StopWatch.stop("test_list_bucket_google")


    #
    def test_benchmark(self):
        Benchmark.print(sysinfo=False, csv=True, tag=cloud)


