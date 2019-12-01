import os
import stat
from pprint import pprint
from cloudmesh.common.dotdict import dotdict

from cloudmesh.storage.StorageNewABC import StorageABC
import oyaml as yaml
from cloudmesh.common.debug import VERBOSE
from cloudmesh.common.StopWatch import StopWatch
from cloudmesh.common.console import Console
from cloudmesh.common.util import banner, path_expand, writefile, readfile
from cloudmesh.common.Printer import Printer
from cloudmesh.configuration.Config import Config
from google.cloud import storage

from pathlib import Path
from glob import glob
import os
import shutil
import json


class Provider(StorageABC):
    """
    Provider class for local storage.
    This class allows transfer of objects from local storage location to a
    Azure blob storage container or gcp bucket.

    Default parameters are read from ~/.cloudmesh/cloudmesh.yaml :

    storage:
        local:
          cm:
            azureblob: true
            blobactive: true
            heading: local_to_CSP
            host: localhost
            kind: local
            label: local_storage
            version: 0.1
            service: storage
          default:
            directory: ~\cmStorage
          credentials:
            userid: None
            password: None
    """

    @staticmethod
    def json_to_yaml(name, filename="~/.cloudmesh/google.json"):
        """
        given a json file downloaded from google, copies the content into the cloudmesh
        jaml file, while overwriting or creating a new storage provider

        :param filename:
        :return:
        """
        # creates cloud,esh.storgae.{name}

        path = path_expand(filename)

        with open(path, "r") as file:
            d = json.load(file)
        config = Config()
        element = {
            "cm": {
                "name": name,
                "active": 'true',
                "heading": "GCP",
                "host": "https://console.cloud.google.com/storage",
                "kind": "storage",
                "cloud": "google",
                "version": "TBD",
                "service": "storage"
            },
            "default": {
                "directory": "cloudmesh_gcp",
                "Location_type": "Region",
                "Location": "us - east1",
                "Default_storage_class": "Standard",
                "Access_control": "Uniform",
                "Encryption": "Google-managed",
                "Link_URL": "https://console.cloud.google.com/storage/browser/cloudmesh_gcp",
                "Link_for_gsutil": "gs://cloudmesh_gcp"
            },
            "credentials": d
        }
        config["cloudmesh"]["storage"][name] = element
        config.save()
        pprint(config["cloudmesh"]["storage"][name])

    @staticmethod
    def yaml_to_json(name, filename="~/.cloudmesh/google.json"):
        """
        given the name in the yaml file, takes the information form that object and creates the
        json file that cna be conveniently used by google
        :param name:
        :param filename:
        :return:
        """
        config = Config()
        configuration = config[f"cloudmesh.storage.{name}"]
        credentials = config[f"cloudmesh.storage.{name}.credentials"]
        # generate json
        writefile(filename, json.dumps(credentials, indent=4))


    @staticmethod
    def delete_json(filename="~/.cloudmesh/google.json"):
        """
        deletes the json file. Make sure you have it saved in the yaml
        :param filename:
        :return:
        """

    def __init__(self,
                 service=None,
                 config="~/.cloudmesh/cloudmesh.yaml",
                 json=None,
                 **kwargs):
        super().__init__(service=service, config=config)

        if json:
            self.path = path_expand(json)
            self.client = storage.Client.from_service_account_json(self.path)

        else:
            self.config = Config(config_path=config)
            self.configuration = self.config[f"cloudmesh.storage.{service}"]
            self.kind = self.config[f"cloudmesh.storage.{service}.cm.kind"]
            self.credentials = dotdict(self.configuration["credentials"])
            self.bucket_name = self.config[f"cloudmesh.storage.{service}.default.directory"]
            self.yaml_to_json(service)
            self.path = path_expand("~/.cloudmesh/google.json")
            self.client = storage.Client.from_service_account_json(self.path)


    # def extract_file_dict(self, filename, metadata):
    #     # print(metadata)
    #     info = {
    #         "fileName": filename,
    #         # "creationDate" : metadata['ResponseMetadata']['HTTPHeaders']['date'],
    #         "lastModificationDate":
    #             metadata['ResponseMetadata']['HTTPHeaders']['last-modified'],
    #         "contentLength":
    #             metadata['ResponseMetadata']['HTTPHeaders']['content-length']
    #     }
    #
    # download_path = path_expand("~/.cloudmesh/download_file")
    # json_path = path_expand("~/.cloudmesh/gcp.json")
    #
    # bucket_name = "cloudmesh_gcp"
    # gcp = storage.Client.from_service_account_json(json_path)


    # def bucket_exists(self, name=None):
    #      bucket = gcp.get_bucket(name)
    #
    #      if bucket == bucket_name:
    #         return  True
    #      else:
    #          return False
    #
    #
    # def bucket_create(self, name=None):
    #
    #     #bucket_name = 'my-new-bucket_shre2'
    #     Creates the new bucket
    #     bucket = storage_client.create_bucket(bucket_name)
    #     print("Bucket Created:", bucket_name)
    #     return True
    #
    #
    #
    #
    #
    # def create_dir(self, directory=None):
    #     bucket = bucket_name
    #     if not self.bucket_exists(name=bucket):
    #         self.bucket_create(name=bucket)
    #     banner("Create a dir in bucket")
    #     # Create a new folder.
    #     folder = directory
    #     #folder = 'a169/a17/'
    #     blob1 = bucket.blob(folder)
    #     blob1.upload_from_string('')


    def list(self, service=None, sourceObj=None, recursive=False):
        raise NotImplementedError

    def delete(self, service="local", sourceObj=None, recursive=False):
        raise NotImplementedError

    def put(self, source=None, destination=None, recursive=False):
        raise NotImplementedError

    def get(self, source=None, destination=None, recursive=False):
        raise NotImplementedError

    def search(self, directory=None, filename=None, recursive=False):
        raise NotImplementedError


