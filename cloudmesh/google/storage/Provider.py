import os
import stat
from pprint import pprint

import boto3
from botocore.exceptions import ClientError
from cloudmesh.storage.StorageNewABC import StorageNewABC
import oyaml as yaml
from cloudmesh.common.debug import VERBOSE
from cloudmesh.common.StopWatch import StopWatch
from cloudmesh.common.console import Console
from cloudmesh.common.util import banner
from cloudmesh.common.Printer import Printer
from cloudmesh.configuration.Config import Config
from google.cloud import storage

from pathlib import Path
from glob import glob
import os
import shutil


class Provider(StorageNewABC):
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

    def __init__(self, service=None, config="~/.cloudmesh/cloudmesh.yaml",
                 **kwargs):
        super().__init__(service=service, config=config)

        if kwargs.get("debug"):
            print("Inside init of local provider")
            print(self.kind)
            print(kwargs.get('sourceObj'))
            print(kwargs.get('target'))
            print(kwargs.get('targetObj'))
            print(self.credentials)

        # Processing --source/service and --target arguments separately.
        # This is a provider class for local storage hence --source/service will \
        # always be "local"

        self.sourceCSP = self.service

        try:
            self.config = Config(config_path=config)
            self.yaml_content_source = self.config["cloudmesh.storage."
                                                   f"{self.sourceCSP}"]
            self.source_kind = self.yaml_content_source["cm"]["kind"]
            self.source_credentials = self.yaml_content_source["credentials"]

            print("Accessing local storage location.")
            if kwargs.get('sourceObj'):
                self.local_location = Path(self.yaml_content_source['default'][
                                               'directory'],
                                           kwargs.get('sourceObj'))
            else:
                self.local_location = self.yaml_content_source['default'][
                    'directory']

            if kwargs.get("debug"):
                print(f"\nLocal location to access {self.local_location}")

            if kwargs.get('target'):
                self.targetCSP = kwargs.get('target')
                self.yaml_content_target = self.config["cloudmesh.storage."
                                                       f"{self.targetCSP}"]
                self.target_kind = self.yaml_content_target["cm"]["kind"]
                self.target_credentials = self.yaml_content_target[
                    "credentials"]
                self.target_container = self.target_credentials["container"]

        except Exception as e:
            Console.error(f"Couldn't access cloudmesh.yaml. Error - {e}")
            return ()

        if kwargs.get("debug"):
            VERBOSE(self.yaml_content_source)
            if kwargs.get('target'):
                VERBOSE(self.yaml_content_target)

        banner(f"Source CSP: {self.source_kind}")
        if kwargs.get('target'):
            banner(f"Target CSP: {self.target_kind}")

        # Creating connection with the target CSP. This done only if the
        # --target argument is provided. Only "copy" command is expected to
        # have --target argument.

        if kwargs.get('target'):
            if self.target_kind == "azureblob":
                print("Create Azure connection.")

                if 'TBD' == self.target_credentials["access_key_id"] \
                        or 'TBD' == self.target_credentials["secret_access_key"] \
                        or 'TBD' == self.target_credentials["region"]:
                    Console.error("Critical details missing from .yaml file. "
                                  "TBD  not allowed. Please check.")

                try:
                    self.s3_client = boto3.client(
                        's3',
                        aws_access_key_id=self.target_credentials[
                            "access_key_id"],
                        aws_secret_access_key=self.target_credentials[
                            "secret_access_key"],
                        region_name=self.target_credentials["region"]
                    )
                    Console.ok(
                        f"Successful connection to {self.target_kind} is "
                        f"made.")
                except ClientError as e:
                    Console.error(e, prefix=True, traceflag=True)

            elif self.kind == "gcpbucket":
                print("Create GCP connection.")
                raise NotImplementedError
            else:
                raise NotImplementedError

    # TODO - check hor to pass recursive argument from master provider & transfer.py

    def list(self, service=None, sourceObj=None, recursive=False):
        """
        Method to enlist all objects of target location.

        :param service: local/azureblob/gcpbucket
        :param sourceObj: source directory or file
        :param recursive: Boolean to indicate if sub components to be enlisted
        :return: list of lists containing objects from target location
        """
        if self.source_kind == "azureblob":
            Console.error("This command should flow to azure provider. Please "
                          "check.")
            return
        elif self.source_kind == "gcpbucket":
            Console.error("This command should flow to gcp provider. Please "
                          "check.")
            return
        elif self.source_kind == "awss3":
            Console.error("This command should flow to AWS provider. Please "
                          "check.")
            return
        elif self.source_kind == "local":
            banner(f"Executing list method for local storage:\nSource object "
                   f"is {self.local_location}")
            if self.local_location.exists():
                if self.local_location.expanduser().is_file():
                    os.chdir(os.path.split(self.local_location.expanduser())[0])

                    if len(glob(sourceObj)) > 0:
                        Console.ok("List of file(s):\n"
                                   f"{self.local_location.expanduser()}")
                    else:
                        Console.error(f"File not found "
                                      f"{self.local_location.expanduser()}")
                elif self.local_location.expanduser().is_dir():
                    os.chdir(self.local_location.expanduser())
                    Console.ok(f"List if files in {self.local_location}:\n")
                    for f in glob("**", recursive=recursive):
                        print(Path.cwd() / f)
            else:
                Console.error(f"Source object {self.local_location} does not "
                              f"exist.")
        else:
            raise NotImplementedError
            return {}

    def delete(self, service="local", sourceObj=None, recursive=False):
        """
        This method deletes file(s) / folder(s) from the source location.

        :param service: "local" for this provider
        :param sourceObj: A file or folder to delete
        :param recursive: Delete files from folder/subfolders
        :return: None
        """
        if self.source_kind == "azureblob":
            Console.error("This command should flow to AWS provider. Please "
                          "check.")
            return
        elif self.source_kind == "gcpbucket":
            Console.error("This command should flow to AWS provider. Please "
                          "check.")
            return
        elif self.source_kind == "local":
            banner(f"Executing delete method for local storage:\nSource object "
                   f"is {self.local_location}")

            if self.local_location.exists():
                if self.local_location.expanduser().is_file():
                    os.chdir(os.path.split(self.local_location.expanduser())[0])

                    if len(glob(sourceObj)) > 0:
                        Console.ok("Following file will be removed:\n"
                                   f"{self.local_location.expanduser()}")
                        os.remove(self.local_location.expanduser())
                    else:
                        Console.error(f"File not found "
                                      f"{self.local_location.expanduser()}")
                elif self.local_location.expanduser().is_dir():
                    os.chdir(self.local_location.expanduser())
                    Console.ok(f"Following objects will be removed from: "
                               f"{self.local_location}:\n")
                    for f in glob("**", recursive=recursive):
                        print(Path.cwd() / f)

                    shutil.rmtree(self.local_location.expanduser())
            else:
                Console.error(
                    f"Source object {self.local_location} does not exist.")
        else:
            raise NotImplementedError
            return {}

    def s3_bucket_exists(self, target_container):
        """    ##FIX CODE FROM Here ##
        Determine whether bucket_name exists and the user has permission to
        access it

        :param target_container: azure blob name
        :return: True if the referenced bucket_name exists, otherwise False
        """
        try:
            resp_exists = self.s3_client.head_bucket(Bucket=target_container)
        except ClientError as e:
            return False
        return True

    def copy(self, service="local", sourceObj="abcd.txt", target="aws",
             targetObj=None, debug=True):
        """
        copy method copies files/directories from local storage to target CSP

        :param service:  "local" for this provider
        :param sourceObj: A file/directory to be copied
        :param targetObj: Name of the target object
        :param debug: Boolean indicating debug mode
        :return: None
        """
        # To copy the whole cmStorage directory, pls provide sourceObj=None

        if self.s3_bucket_exists(self.target_container):
            Console.ok(f"AWS S3 bucket {self.target_container} exists.")

            # TODO : Check CLI option
            # CLI option
            # aws s3 cp C:\Users\kpimp\cmStorage
            # s3://bucket-iris.json/cmStorage --recursive

            if sourceObj:
                self.local_location = Path(self.yaml_content_source['default'][
                                               'directory'], sourceObj)
                print("=====> local location ", self.local_location)
            else:
                sourceObj = "cmStorage"
                self.local_location = self.yaml_content_source['default'][
                    'directory']

            if targetObj is None:
                targetObj = sourceObj

            source_path = Path(self.local_location)

            try:
                if source_path.expanduser().is_file():
                    # TODO: Use queue here
                    print(f"Copying file. Pushed {source_path} to the queue.")
                    os.chdir(os.path.split(self.local_location.expanduser())[0])
                    print("chdir to ", os.getcwd())
                    try:
                        response = self.s3_client.upload_file(sourceObj,
                                                              self.target_container,
                                                              targetObj)
                        Console.ok(f"Uploaded: {sourceObj}")
                    except ClientError as e:
                        Console.error(f"Error while uploading {source_path} "
                                      f"to S3 bucket: \n", e)
                elif source_path.expanduser().is_dir():
                    print("Copying directory recursively")
                    os.chdir(source_path.expanduser())
                    print("chdir to ", os.getcwd())

                    for file in glob('**', recursive=True):
                        # TODO: This creates files as foldername/filename
                        # Check how to create directory structure in S3

                        print(file)
                        if Path(file).is_file():
                            # TODO: Use queue here
                            print(f"Pushed {Path(file)} to the queue {file}.")
                            targetObj = file
                            try:
                                response = self.s3_client.upload_file(file,
                                                                      self.target_container,
                                                                      targetObj)
                                Console.ok(f"Uploaded: {file}")
                            except ClientError as e:
                                Console.error(
                                    f"Error while uploading {source_path} "
                                    f"to S3 bucket: \n", e)
                else:
                    Console.error(f"Invalid source object type: {source_path}")
                    return
            except Exception as e:
                print(e)

        else:
            Console.error(f"AWS S3 bucket {self.target_container} does not "
                          f"exist.")


def main():
    print("Instantiating")
    # following instantiating for copy command
    instance = Provider(service="local", sourceObj="abcd.txt", target="aws",
                        targetObj=None, debug=True)

    # Instantiating for list/delete command
    # instance = Provider(service="local", sourceObj="a",
    #                    targetObj=None, debug=True)

    # instance.list(service="local", sourceObj="a", recursive=True)

    # instance.delete(service="local", sourceObj="a", recursive=True)

    instance.copy(service="local", sourceObj='abcd.txt', target="aws",
                  targetObj=None, debug=True)


if __name__ == "__main__":
    main()
