import os
import stat
from pprint import pprint
from cloudmesh.common.dotdict import dotdict

from cloudmesh.storage.StorageNewABC import StorageABC
import oyaml as yaml
from cloudmesh.common.debug import VERBOSE
from cloudmesh.common.StopWatch import StopWatch
from cloudmesh.common.console import Console
from cloudmesh.common.util import banner, path_expand
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
        given a json file downloaded from google, copies the content into the cloudmesh jaml file, while overwriting or creating a new storage provider
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
                "kind": "storage",
                "cloud": "google",

            },
            "default": {
                "directory": "cloudmesh_gcp"
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
        raise NotImplementedError

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
