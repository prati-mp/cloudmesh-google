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
from cloudmesh.common.variables import Variables
from google.cloud import storage

from pathlib import Path
from glob import glob
import os
import shutil
import json
import logging


class Provider(StorageABC):


    @staticmethod
    def json_to_yaml(name, filename="~/.cloudmesh/google.json"):
        """
        given a json file downloaded from google, copies the content into the cloudmesh yaml file, while overwriting or creating a new storage provider
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
                "kind": "google",
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
        print ("AAAA")
        config = Config()
        configuration = config[f"cloudmesh.storage.{name}"]
        credentials = config[f"cloudmesh.storage.{name}.credentials"]
        # generate json

        writefile(filename, json.dumps(credentials, indent=2) + "\n")

    @staticmethod
    def delete_json(filename="~/.cloudmesh/google.json"):
        """
        deletes the json file. Make sure you have it saved in the yaml
        :param filename:
        :return:
        """
        raise NotImplementedError

    def __init__(self,
                 service=None,
                 config="~/.cloudmesh/cloudmesh.yaml",
                 json=None,
                 **kwargs):
        super().__init__(service=service, config=config)
        variables=Variables()
        self.debug=variables['debug']



        if json:
            self.path = path_expand(json)
            self.client = storage.Client.from_service_account_json(self.path)

        else:
            self.config = Config(config_path=config)
            self.configuration = self.config[f"cloudmesh.storage.{service}"]
            self.kind = self.config[f"cloudmesh.storage.{service}.cm.kind"]
            self.credentials = dotdict(self.configuration["credentials"])
            self.bucket_name = self.config[f"cloudmesh.storage.{service}.default.directory"]
           # self.yaml_to_json(service)
            self.path = path_expand("~/.cloudmesh/google.json")
            print("11111:",self.path)
            print("bucketName:", self.bucket_name)
            self.client = storage.Client.from_service_account_json(self.path) #Important for goole login
            self.storage_dict = {}
            self.bucket = self.client.get_bucket(self.bucket_name)

    def get(self, source=None, destination=None, recursive=False):
        self.storage_dict['source'] = source  # src
        self.storage_dict[f'destination/{source}'] = destination

        if self.debug:
            print ("GET")
            print (f"Source {self.bucket}:{source}")
            print(f"Destination local:{destination}")


        try:
            # Excluding any directory from the bucket.
            #filter and list the files which need to download using Google Storage bucket.list_blobs function.
            # List all objects that satisfy the filter.
            delimiter = '/'
            blobs = self.bucket.list_blobs(prefix=source, delimiter=delimiter)
            if self.debug:
                print("Blobs in google bucket(files/folders):", blobs)
            if not os.path.exists(destination):
                os.makedirs(destination)
            for blob in blobs:
                destination_uri = f'{destination}/{blob.name}'

                logging.info(f'Blobs: {blob.name}')
                print(f'{blob.name} ->  {destination_uri}. downloading. ', end='')
                blob.download_to_filename(path_expand(destination_uri))
                print('ok.')

        except Exception as e:
            print('Failed to download : ' + str(e))



    def put(self, source=None, destination=None, recursive=False):

        print(self.bucket)
        self.storage_dict['action'] = 'put'
        self.storage_dict['source'] = source
        self.storage_dict['destination'] = destination  # dest
        print(self.bucket)
        print(source)
        print(destination)
        blob = self.bucket.blob(destination)
        blob.upload_from_filename(path_expand(source))
        print(f'File {source} uploaded to {destination}.'.format(source, destination))

    def list(self, source=None, dir_only=False, recursive=False):

        self.storage_dict['source'] = source
        print(self.bucket)
        print(source)
        blobs = self.client.list_blobs(self.bucket_name, prefix=source)
        print('Blobs:')
        print(blobs)
        for blob in blobs:
            print(blob.name)


    def delete(self, source=None):
        """Deletes a blob from the bucket."""
        self.storage_dict['source'] = source
        print("Source=====>", source)
        try:
            blobs = self.bucket.list_blobs(prefix=source)
            print("blobs=====>",blobs )
            for blob in blobs:
                print("Blobs in loop=====>", blob.name)
                blob.delete()
                print('Blob deleted {}'.format(blob.name))
        except Exception as e:
            print('Failed to delete blob at google bucket: ' + str(e))

    def create_dir(self, directory=None):
        self.storage_dict['directory'] = directory
        print("Directory or folder =====>", directory)
        try:
            print("Create a directory/folder in bucket",self.bucket_name)
            blob1 = self.bucket.blob(directory)
            blob1.upload_from_string('')
            print('{} '.format(blob1.name))
        except Exception as e:
            print('Failed to create directory at google bucket: ' + str(e))


    def blob_metadata(self, blob_name=None):
        """Prints out a blob's metadata."""
        self.storage_dict['blob_name'] = blob_name
        print("blob_name  =====>", blob_name)
        try:
            print("Bucket:  ", self.bucket)
            blob = self.bucket.get_blob(blob_name)
            print("blob :", blob)

            print('Blob: {}'.format(blob.name))
            print('Bucket: {}'.format(blob.bucket.name))
            print('Storage class: {}'.format(blob.storage_class))
            print('ID: {}'.format(blob.id))
            print('Size: {} bytes'.format(blob.size))
            print('Updated: {}'.format(blob.updated))
            print('Generation: {}'.format(blob.generation))
            print('Metageneration: {}'.format(blob.metageneration))
            print('Etag: {}'.format(blob.etag))
            print('Owner: {}'.format(blob.owner))
            print('Component count: {}'.format(blob.component_count))
            print('Crc32c: {}'.format(blob.crc32c))
            print('md5_hash: {}'.format(blob.md5_hash))
            print('Cache-control: {}'.format(blob.cache_control))
            print('Content-type: {}'.format(blob.content_type))
            print('Content-disposition: {}'.format(blob.content_disposition))
            print('Content-encoding: {}'.format(blob.content_encoding))
            print('Content-language: {}'.format(blob.content_language))
            print('Metadata: {}'.format(blob.metadata))
            print("Temporary hold: ",
                  'enabled' if blob.temporary_hold else 'disabled')
            print("Event based hold: ",
                  'enabled' if blob.event_based_hold else 'disabled')
            if blob.retention_expiration_time:
                print("retentionExpirationTime: {}".format(blob.retention_expiration_time))

        except Exception as e:
            print('Failed to find blob metadata : ' + str(e))

            """Renames a blob."""
    def rename_blob(self, blob_name=None, new_name=None):
        self.storage_dict['blob_name'] = blob_name
        self.storage_dict['new_name'] = new_name
        print("blob_name  =====>", blob_name)
        print("new_name  =====>", new_name)
        try:
            print("Bucket:  ", self.bucket)
            blob = self.bucket.blob(blob_name)
            print("blob:  ", blob)
            new_blob = self.bucket.rename_blob(blob, new_name)
            print("new blob:  ", new_blob)
            print(f'Blob {blob_name} has been renamed to {new_blob}'.format(blob.name, new_blob.name))
        except Exception as e:
            print('Failed to rename blob  : ' + str(e))

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
