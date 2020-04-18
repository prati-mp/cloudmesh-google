from pprint import pprint
import json
import logging
import os
from pprint import pprint

from cloudmesh.common.dotdict import dotdict
from cloudmesh.common.util import path_expand, writefile
from cloudmesh.common.variables import Variables
from cloudmesh.configuration.Config import Config
from cloudmesh.abstract.StorageABC import StorageABC
from google.cloud import storage


class Provider(StorageABC):


    sample = """
    cloudmesh:
      storage:
        {name}:
          cm:
            name: google
            active: 'true'
            heading: GCP
            host: https://console.cloud.google.com/storage
            kind: google
            version: TBD
            service: storage
          default:
            directory: {bucket}
            Location_type: Region
            Location: us - east1
            Default_storage_class: Standard
            Access_control: Uniform
            Encryption: Google-managed
            Link_URL: https://console.cloud.google.com/storage/browser/{bucket}
            Link_for_gsutil: gs://{bucket}
          credentials:
            type: service_account
            project_id: imposing-coast-123456
            private_key_id: a1b2c3d4*********
            private_key: TBD
            client_email: TBD
            client_id: TBD
            auth_uri: https://accounts.google.com/o/oauth2/auth
            token_uri: https://oauth2.googleapis.com/token
            auth_provider_x509_cert_url: https://www.googleapis.com/oauth2/v1/certs
            client_x509_cert_url: TBD
    """
    @staticmethod
    def get_filename(filename):
        if filename.startswith("./"):

            _filename = filename[2:]

        elif filename.startswith("."):
            _filename = filename[1:]
        else:
            _filename = filename

        return _filename

    @staticmethod
    def get_kind():
        kind = ["google"]
        return kind

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
        #
        # BUG START FROM THE sample
        #
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
        # print ("AAAA")
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
                 json=None,
                 **kwargs):
        super().__init__(service=service)
        variables=Variables()
        self.debug=variables['debug']

        if json:
            self.path = path_expand(json)
            self.client = storage.Client.from_service_account_json(self.path)

        else:
            self.config = Config()
            self.configuration = self.config[f"cloudmesh.storage.{service}"]
            self.kind = self.config[f"cloudmesh.storage.{service}.cm.kind"]
            self.credentials = dotdict(self.configuration["credentials"])
            self.bucket_name = self.config[f"cloudmesh.storage.{service}.default.directory"]
           # self.yaml_to_json(service)
            self.path = path_expand("~/.cloudmesh/google.json")
            #print("11111:",self.path)
            #print("bucketName:", self.bucket_name)
            self.client = storage.Client.from_service_account_json(self.path) #Important for goole login
            self.storage_dict = {}
            self.bucket = self.client.get_bucket(self.bucket_name)

    def get(self, source=None, destination=None, recursive=False):
        """
         Downloads(get) the source(bucket blob) to local storage
         :param source: the source which either can be a directory or file
         :param destination: the destination which either can be a directory or file
         :return: dict

         """

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

    def put(self, source=None, destination=None, recursive=None ):
        """
        Uploads(puts) the source(local) to the destination service bucket
        :param source: the source which either can be a directory or file
        :param destination: the destination which either can be a directory or file
        :return: dict

        """
        print(self.bucket)
        self.storage_dict['action'] = 'put'
        self.storage_dict['source'] = source
        self.storage_dict['destination'] = destination  # dest
        try:
            print("Bucket: ",self.bucket)
            print("Source: ",source)
            print("Destination: ",destination)
            blob = self.bucket.blob(destination)
            blob.upload_from_filename(path_expand(source))
            print(f'File {source} uploaded to {destination}.'.format(source, destination))
        except Exception as e:
            print('Failed to upload blob at google bucket: ' + str(e))

    def list(self, source=None, dir_only=False, recursive=False):
        """
        Lists the source: google bucket blob(s) with and without prefix
        :param source: the source which either can be a directory or file (either provide fill path or a prefix)
        :return: dict

        """
        self.storage_dict['source'] = Provider.get_filename(source)
        print("Bucket: ",self.bucket)
        print("Source keyword: ",source)
        try:
            blobs = self.client.list_blobs(self.bucket_name, prefix=self.storage_dict['source'])
            print('Blobs: ')
            print(blobs)
            for blob in blobs:
                print(blob.name)
        except Exception as e:
            print('Failed to list blobs from google bucket: ' + str(e))

    def delete(self, source=None):
        """
        Deletes a blob from the bucket.
        :param source: Enter the blob name at google bucket you like to delete
        :return: dict

        """
        self.storage_dict['source'] = source
        # print("Source=====>", source)
        try:
            blobs = self.bucket.list_blobs(prefix=source)
            # print("blobs=====>",blobs )
            for blob in blobs:
                # print("Blobs in loop : ", blob.name)
                blob.delete()
                print('Blob deleted {}'.format(blob.name))
        except Exception as e:
            print('Failed to delete blob at google bucket: ' + str(e))

    def create_dir(self, directory=None):
        """
        Creates a directory or folder at google bucket.
        :param directory: Enter the directory structure  you like to create at google bucket
        :return: dict

        """
        self.storage_dict['directory'] = directory
        print("Provided Directory or folder : ", directory)
        try:
            # print("Create a directory or folder in bucket ",self.bucket_name)
            blob1 = self.bucket.blob(directory)
            blob1.upload_from_string('')
            print('directory or folder name : {} '.format(blob1.name))
        except Exception as e:
            print('Failed to create directory at google bucket: ' + str(e))


    def blob_metadata(self, blob_name=None):
        """
        Prints out a blob's metadata.
        :param blob_name: Enter the blob name with full path at google bucket you like to get metadata
        :return: dict

        """
        self.storage_dict['blob_name'] = blob_name
        try:
            print('Bucket : {} '.format(self.bucket.name))
            blob = self.bucket.get_blob(blob_name)
            # print("blob  : ", blob)
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


    def rename_blob(self, blob_name=None, new_name=None):
        """
        Renames a blob at google bucket
        :param blob_name: Enter the existing blob name with full path at google bucket
        :param new_name: Enter the new blob name with full path at google bucket you like to rename
        :return: dict

        """
        self.storage_dict['blob_name'] = blob_name
        self.storage_dict['new_name'] = new_name
        # print("original blob_name :", blob_name)
        print("new_name  :", new_name)
        try:
            print("Bucket:  ", self.bucket)
            blob = self.bucket.blob(blob_name)
            # print("blob:  ", blob)
            new_blob = self.bucket.rename_blob(blob, new_name)
            # print("new blob:  ", new_blob)
            print('Blob {} has been renamed to {}'.format(blob.name, new_blob.name))
        except Exception as e:
            print('Failed to rename blob  : ' + str(e))


    def create_bucket(self,new_bucket_name=None):
        """
        Creates a new bucket, only used for creating new bucket
        :param new_bucket_name: Enter the name of new bucket yoy like to create at google cloud
        :return: dict

        """
        self.storage_dict['new_bucket_name'] = new_bucket_name
        try:
            bucket_new = self.client.create_bucket(new_bucket_name)
            print(f'Bucket {new_bucket_name} created'.format(bucket_new.name))
        except Exception as e:
            print('Failed to create new google bucket  : ' + str(e))

    def list_bucket(self):
        """
        Lists google cloud bucket, only used for listing bucket
        :return: dict

        """
        try:
            buckets = self.client.list_buckets()
            for bucket in buckets:
                print(bucket.name)
        except Exception as e:
            print('Failed to list  google buckets : ' + str(e))

    def copy_blob_btw_buckets(self, blob_name, bucket_name_dest, blob_name_dest):
        """
        Copies a blob from one bucket to another with a new name.
        :param blob_name: Enter the blob name with full path at google bucket you like to copy
        :param bucket_name_dest: Enter the destination google cloud bucket name you like to copy blob
        :param blob_name_dest: Enter the new destination blob name with full path at destination google bucket
        :return: dict

        """
        self.storage_dict['blob_name'] = blob_name
        self.storage_dict['bucket_name_dest'] = bucket_name_dest
        self.storage_dict['blob_name_dest'] = blob_name_dest
        try:
            # source_bucket = self.bucket
            # source_bucket = self.client.get_bucket(bucket_name)
            source_blob = self.bucket.blob(blob_name)
            destination_bucket = self.client.get_bucket(bucket_name_dest)
            dest_blob = self.bucket.copy_blob(
                source_blob, destination_bucket, blob_name_dest)
            print(f'Source Bucket:{self.bucket} ,   destination Bucket:{destination_bucket}')
            print(f'Blob {source_blob}  copied to blob {dest_blob} .'.format(source_blob.name,  dest_blob.name))
        except Exception as e:
            print('Failed to copy blob to destination google bucket  : ' + str(e))

    def search(self, directory=None, filename=None, recursive=False):
        """
        gets the destination and copies it in source

        :param service: the name of the service in the yaml file
        :param directory: the directory which either can be a directory or file
        :param filename: filename
        :param recursive: in case of directory the recursive refers to all
                          subdirectories in the specified source
        :return: dict
        """
        raise NotImplementedError

    def sync(self, source=None, destination=None, recursive=None ):
        """
        sync the destination and local

        :param source:  local computer location
        :param destination: cloud service
        :param recursive: in case of directory the recursive refers to all
                          subdirectories in the specified source
        :return: dict
        """
        raise NotImplementedError

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
