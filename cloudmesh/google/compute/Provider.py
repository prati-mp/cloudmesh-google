import json
import time
from pprint import pprint

import yaml
from cloudmesh.abstract.ComputeNodeABC import ComputeNodeABC
from cloudmesh.common.console import Console
from cloudmesh.common.util import banner
from cloudmesh.common.util import path_expand
from cloudmesh.configuration.Config import Config
from cloudmesh.provider import ComputeProviderPlugin
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class Provider(ComputeNodeABC, ComputeProviderPlugin):
    kind = 'google'

    sample = """
            cloudmesh:
              cloud:
                {name}:
                  cm:
                    active: true
                    heading: {name}
                    host: https://console.cloud.google.com/compute/instances?project={project_id}
                    label: {name}
                    kind: google
                    version: v1
                    service: compute
                  default:
                    image: ubuntu-1910
                    image-project: ubuntu-os-cloud
                    storage_bucket: cloudmesh-bucket
                    zone: us-west3-a
                    type: n1-standard-1
                    resource_group: cloudmesh
                    network: global/networks/default
                  credentials:
                    type: {type}
                    auth:
                        json_file: {filename}
                        project_id: {project_id}
                        client_email: {client_email}
            """

    # Google VM Statuses.
    vm_state = [
        'PROVISIONING',
        'STAGING',
        'RUNNING',
        'STOPPING',
        'STOPPED',
        'SUSPENDING',
        'SUSPENDED',
        'TERMINATED'
    ]

    output = {
        "status": {
            "sort_keys": ["cm.name"],
            "order": ["cm.name",
                      "cm.kind",
                      "status",
                      "id"
                      "zone"],
            "header": ["Name",
                       "Cloud",
                       "Status",
                       "ID",
                       "Zone"]
        },
        "vm": {
            "sort_keys": ["cm.name"],
            "order": [
                "cm.name",
                "cm.cloud",
                "cm.kind",
                "id",
                "type",
                "zone",
                "status",
                "deviceName",
                "diskSizeGb",
                "sourceImage",
                "type",
                "mode",
                "created"],
            "header": [
                "Name",
                "Cloud",
                "Kind",
                "Id",
                "Type",
                "Zone",
                "Status",
                "Disk Name",
                "OS Disk Size",
                "OS Name",
                "Disk Type",
                "Provisioning State",
                "Created"]
        },
        "image": {
            "sort_keys": ["cm.name",
                          "plan.publisher"],
            "order": ["cm.name",
                      "location",
                      "plan.publisher",
                      "plan.name",
                      "plan.product",
                      "operating_system"],
            "header": ["cm.Name",
                       "Location",
                       "Publisher",
                       "Plan Name",
                       "Product",
                       "Operating System",
                       ]
        },
        "flavor": {
            "sort_keys": ["cm.name",
                          "number_of_cores",
                          "os_disk_size_in_mb"],
            "order": ["cm.name",
                      "number_of_cores",
                      "os_disk_size_in_mb",
                      "resource_disk_size_in_mb",
                      "memory_in_mb",
                      "max_data_disk_count"],
            "header": ["Name",
                       "NumberOfCores",
                       "OS_Disk_Size",
                       "Resource_Disk_Size",
                       "Memory",
                       "Max_Data_Disk"]},
        # "status": {},
        "key": {},  # we need this for printing tables
        "secgroup": {},  # we need this for printing tables
        "secrule": {},  # we need this for printing tables
    }

    @staticmethod
    def get_kind():
        kind = ["google"]
        return kind

    def __init__(self, name, configuration):
        cloud = name
        path = configuration
        config = Config(config_path=path)["cloudmesh"]
        self.cm = config["cloud"][cloud]["cm"]
        self.default = config["cloud"][cloud]["default"]
        self.credentials = config["cloud"][cloud]["credentials"]
        self.auth = self.credentials['auth']
        self.compute_scopes = ['https://www.googleapis.com/auth/compute',
                               'https://www.googleapis.com/auth/cloud-platform',
                               'https://www.googleapis.com/auth/compute.readonly']

    def _get_credentials(self, client_secret_file, scopes):
        """
        Method to get the credentials using the Service Account JSON file.
        :param client_secret_file: Service Account JSON File path.
        :param scopes: Scopes needed to provision.
        :return:
        """
        # Authenticate using service account.
        _credentials = service_account.Credentials.from_service_account_file(
            filename=client_secret_file,
            scopes=scopes)
        return _credentials

    def _get_compute_service(self):
        """
            Method to get compute service.
        """
        service_account_credentials = self._get_credentials(
            self.auth['json_file'],
            self.compute_scopes)
        # Authenticate using service account.
        if service_account_credentials is None:
            print('Credentials are required')
            raise ValueError('Cannot Authenticate without Credentials')
        else:
            compute_service = build(self.cm["service"],
                                    self.cm["version"],
                                    credentials=service_account_credentials)

        return compute_service

    def _process_status(self, instance):
        instance_dict = self._process_instance(instance)
        status_dict = {}
        status_dict["cm.name"] = instance_dict["cm.name"]
        status_dict["cm.kind"] = instance_dict["cm.kind"]
        status_dict["status"] = instance_dict["status"]
        status_dict["id"] = instance_dict["id"]
        status_dict["zone"] = instance_dict["zone"]
        return status_dict

    def _process_instance(self, instance):
        """
        Method to convert the instance json to dict.
        :param instance: JSON with instance details
        :return: 
        """
        instance_dict = {}
        ins_zone = instance["zone"]
        instance_dict["zone"] = ins_zone[
                                ins_zone.index("zones/") + 6:len(ins_zone)]
        instance_dict["cm.name"] = instance["name"]
        instance_dict["cm.cloud"] = self.kind
        instance_dict["status"] = instance["status"]
        instance_dict["type"] = instance["cpuPlatform"]
        instance_dict["created"] = instance["creationTimestamp"]
        instance_dict["id"] = instance["id"]
        instance_dict["cm.kind"] = instance["kind"]
        machineTypeUrl = instance["machineType"]
        instance_dict["machineType"] = machineTypeUrl[machineTypeUrl.index(
            "machineTypes/") + 13:len(machineTypeUrl)]
        disks = instance["disks"]
        disk = disks[0]
        instance_dict["deviceName"] = disk["deviceName"]
        instance_dict["diskSizeGb"] = disk["diskSizeGb"]
        licenses = disk["licenses"][0]
        instance_dict["sourceImage"] = licenses[
                                       licenses.index("licenses/") + 9:len(
                                           licenses)]
        instance_dict["type"] = disk["type"]
        instance_dict["mode"] = disk["mode"]

        return instance_dict

    def update_dict(self, elements, kind=None):
        """
        This function adds a cloudmesh cm dict to each dict in the list
        elements.

        returns an object or list of objects With the dict method
        this object is converted to a dict. Typically this method is used
        internally.

        :param elements: the list of original dicts. If elements is a single
                         dict a list with a single element is returned.
        :param kind: for some kinds special attributes are added. This includes
                     key, vm, image, flavor.
        :return: The list with the modified dicts
        """

        if elements is None:
            return None
        elif type(elements) == list:
            _elements = elements
        else:
            _elements = [elements]
        d = []
        for entry in _elements:

            if "cm" not in entry:
                entry['cm'] = {}

            if kind == 'ip':
                entry['name'] = entry['floating_ip_address']

            entry["cm"].update({
                "kind": kind,
                "driver": self.cloudtype,
                "cloud": self.cloud,
                "name": entry['name']
            })

            if kind == 'key':

                try:
                    entry['comment'] = entry['public_key'].split(" ", 2)[2]
                except:
                    entry['comment'] = ""
                entry['format'] = \
                    entry['public_key'].split(" ", 1)[0].replace("ssh-", "")

            elif kind == 'vm':

                entry["cm"]["updated"] = str(DateTime.now())

                if 'public_v4' in entry:
                    entry['ip_public'] = entry['public_v4']

                if "created_at" in entry:
                    entry["cm"]["created"] = DateTime.utc(entry["created_at"])
                    # del entry["created_at"]
                    if 'status' in entry:
                        entry["cm"]["status"] = str(entry["status"])
                else:
                    entry["cm"]["created"] = entry["modified"]

            elif kind == 'flavor':

                entry["cm"]["created"] = entry["updated"] = str(
                    DateTime.now())

            elif kind == 'image':

                entry["cm"]["created"] = entry["updated"] = str(
                    DateTime.now())

            # elif kind == 'secgroup':
            #    pass

            d.append(entry)
        return d

    def _format_aggregate_list(self, instance_list):
        """
        Method to format the instance list to flat dict format.
        :param instance_list: 
        :return: dict
        """
        result = []
        if instance_list is not None:
            if "items" in instance_list:
                items = instance_list["items"]
                for item in items:
                    if "instances" in items[item]:
                        instances = items[item]["instances"]
                        for instance in instances:
                            # Extract the instance details.
                            result.append(self._process_instance(instance))

        return result

    def _wait_for_operation(self, compute_service, operation, project,
                            zone=None, name=None):

        operation_name = operation["name"]
        operation_type = operation["operationType"]
        Console.info(
            f'Waiting for {operation_type} operation to finish : {operation_name}')

        try:
            while True:
                if zone is None:
                    result = compute_service.globalOperations().get(
                        project=project,
                        operation=operation_name).execute()
                else:
                    result = compute_service.zoneOperations().get(
                        project=project,
                        zone=zone,
                        operation=operation_name).execute()

                if result['status'] == 'DONE':
                    if 'error' in result:
                        Console.error("Error in operation")
                        raise Exception(result['error'])
                    else:
                        break

                time.sleep(1)
        except Exception as se:
            raise se

        if name is None:
            msg = f"{operation_type} is complete."
        else:
            msg = f"{operation_type} on {name} is complete."

        Console.ok(msg)

        return result

    def start(self, name=None):
        """
        start a node

        :param name: the unique node name
        :return:  The dict representing the node
        """
        result = None
        compute_service = self._get_compute_service()
        _operation = None
        if name is None:
            Console.error("Instance name is required to start.")
            return
        try:

            project_id = self.auth["project_id"]
            zone = self.default["zone"]
            _operation = compute_service.instances().start(project=project_id,
                                                           zone=zone,
                                                           instance=name).execute()

            self._wait_for_operation(compute_service,
                                     _operation,
                                     project_id,
                                     zone,
                                     name)

            # Get the instance details to update DB.
            result = self.info(name)

        except Exception as se:
            print(se)
            if type(se) == HttpError:
                Console.error(
                    f'Unable to start instance {name}. Reason: {se._get_reason()}')
            else:
                Console.error(f'Unable to start instance {name}.')

        return result

    def stop(self, name=None):
        """
        stops the node with the given name

        :param name:
        :return: The dict representing the node including updated status
        """
        result = None
        compute_service = self._get_compute_service()
        _operation = None
        if name is None:
            return
        try:

            project_id = self.auth["project_id"]
            zone = self.default["zone"]

            _operation = compute_service.instances().stop(
                project=project_id,
                zone=zone,
                instance=name).execute()

            self._wait_for_operation(compute_service,
                                     _operation,
                                     project_id,
                                     zone,
                                     name)

            # Get the instance details to update DB.
            result = self.info(name)

        except Exception as se:
            print(se)
            if type(se) == HttpError:
                Console.error(
                    f'Unable to stop instance {name}. Reason: {se._get_reason()}')
            else:
                Console.error(f'Unable to stop instance {name}.')

        return result

    def info(self, name=None):
        """
        gets the information of a node with a given name

        :param name:
        :return: The dict representing the node including updated status
        """
        result = None
        if name is None:
            Console.error("Instance name is required to start.")
            return
        try:
            banner("Here Here")
            project_id = self.auth["project_id"]
            zone = self.default["zone"]
            compute_service = self._get_compute_service()

            # Get the instance details to update DB.
            result = compute_service.instances().get(project=project_id,
                                                     zone=zone,
                                                     instance=name).execute()
            print(result)
            result = self._process_instance(result)

            result = self.update_dict(result, kind="vm")

        except Exception as se:
            if type(se) == HttpError:
                Console.error(
                    f'Unable to get instance {name} info. Reason: {se._get_reason()}')
            else:
                Console.error(f'Unable to start instance {name}.')

        return result

    def suspend(self, name=None):
        """
        suspends the node with the given name

        :param name: the name of the node
        :return: The dict representing the node
        """
        raise NotImplementedError

    def list(self, **kwargs):
        """
        list all vms

        :return: an array of dicts representing the nodes
        """
        result = None
        try:
            compute_service = self._get_compute_service()

            aggregatedList = compute_service.instances().aggregatedList(
                project=self.auth["project_id"],
                orderBy="name").execute()

            result = self._format_aggregate_list(aggregatedList)

        except Exception as se:
            print(se)

        return result

    def resume(self, name=None):
        """
        resume the named node

        :param name: the name of the node
        :return: the dict of the node
        """
        raise NotImplementedError

    def destroy(self, name=None):
        """
        Destroys the node
        :param name: the name of the node
        :return: the dict of the node
        """
        raise NotImplementedError

    def create(self,
               name=None,
               image=None,
               size=None,
               timeout=360,
               group=None,
               **kwargs):
        """
        creates a named node

        :param group: a list of groups the vm belongs to
        :param name: the name of the node
        :param image: the image used
        :param size: the size of the image
        :param timeout: a timeout in seconds that is invoked in case the
               image does not boot.
               The default is set to 3 minutes.
        :param kwargs: additional arguments passed along at time of boot
        :return:
        """
        """
        create one node
        """
        raise NotImplementedError

    def set_server_metadata(self, name, **metadata):
        """
        sets the metadata for the server

        :param name: name of the fm
        :param metadata: the metadata
        :return:
        """
        raise NotImplementedError

    def get_server_metadata(self, name):
        """
        gets the metadata for the server

        :param name: name of the fm
        :return:
        """
        raise NotImplementedError

    def delete_server_metadata(self, name):
        """
        gets the metadata for the server

        :param name: name of the fm
        :return:
        """
        raise NotImplementedError

    def rename(self, name=None, destination=None):
        """
        rename a node

        :param destination:
        :param name: the current name
        :return: the dict with the new name
        """
        # if destination is None, increase the name counter and use the new name
        raise NotImplementedError

    def keys(self):
        """
        Lists the keys on the cloud

        :return: dict
        """
        raise NotImplementedError

    def key_upload(self, key=None):
        """
        uploads the key specified in the yaml configuration to the cloud
        :param key:
        :return:
        """
        raise NotImplementedError

    def key_delete(self, name=None):
        """
        deletes the key with the given name
        :param name: The name of the key
        :return:
        """
        raise NotImplementedError

    def images(self, **kwargs):
        """
        Lists the images on the cloud
        :return: dict
        """
        raise NotImplementedError

    def image(self, name=None):
        """
        Gets the image with a given name
        :param name: The name of the image
        :return: the dict of the image
        """
        raise NotImplementedError

    def flavors(self, **kwargs):
        """
        Lists the flavors on the cloud

        :return: dict of flavors
        """
        raise NotImplementedError

    def flavor(self, name=None):
        """
        Gets the flavor with a given name
        :param name: The name of the flavor
        :return: The dict of the flavor
        """
        raise NotImplementedError

    def reboot(self, name=None):
        """
        Reboot a list of nodes with the given names

        :param name: A list of node names
        :return:  A list of dict representing the nodes
        """
        raise NotImplementedError

    def attach_public_ip(self, name=None, ip=None):
        """
        adds a public ip to the named vm

        :param name: Name of the vm
        :param ip: The ip address
        :return:
        """
        raise NotImplementedError

    def detach_public_ip(self, name=None, ip=None):
        """
        adds a public ip to the named vm

        :param name: Name of the vm
        :param ip: The ip address
        :return:
        """
        raise NotImplementedError

    def delete_public_ip(self, ip=None):
        """
        Deletes the ip address

        :param ip: the ip address, if None than all will be deleted
        :return:
        """
        raise NotImplementedError

    def list_public_ips(self, available=False):
        """
        Lists the public ip addresses.

        :param available: if True only those that are not allocated will be
            returned.

        :return:
        """
        raise NotImplementedError

    def create_public_ip(self):
        """
        Creates a new public IP address to use

        :return: The ip address information
        """
        raise NotImplementedError

    def find_available_public_ip(self):
        """
        Returns a single public available ip address.

        :return: The ip
        """
        raise NotImplementedError

    def get_public_ip(self, name=None):
        """
        returns the public ip

        :param name: name of the server
        :return:
        """
        raise NotImplementedError

    def list_secgroups(self, name=None):
        """
        List the named security group

        :param name: The name of the group, if None all will be returned
        :return:
        """

    def list_secgroup_rules(self, name='default'):
        """
        List the named security group

        :param name: The name of the group, if None all will be returned
        :return:
        """
        raise NotImplementedError

    def upload_secgroup(self, name=None):
        raise NotImplementedError

    def add_secgroup(self, name=None, description=None):
        raise NotImplementedError

    def add_secgroup_rule(self,
                          name=None,  # group name
                          port=None,
                          protocol=None,
                          ip_range=None):
        raise NotImplementedError

    def remove_secgroup(self, name=None):
        raise NotImplementedError

    def add_rules_to_secgroup(self, name=None, rules=None):
        raise NotImplementedError

    def remove_rules_from_secgroup(self, name=None, rules=None):
        raise NotImplementedError

    def wait(self,
             vm=None,
             interval=None,
             timeout=None):
        """
        wait till the given VM can be logged into

        :param vm: name of the vm
        :param interval: interval for checking
        :param timeout: timeout
        :return:
        """
        raise NotImplementedError
        return False

    def console(self, vm=None):
        """
        gets the output from the console

        :param vm: name of the VM
        :return:
        """
        raise NotImplementedError
        return ""

    def log(self, vm=None):
        raise NotImplementedError
        return ""

    @staticmethod
    def json_to_yaml(cls, name, filename="~/.cloudmesh/security/google.json"):
        """
        Given a json file downloaded from google, copies the content into the
        cloudmesh yaml file, while overwriting or creating a new compute provider
        :param cls:
        :param name:
        :param filename: Service Account Key file downloaded from google cloud.
        :return: None
        """
        path = path_expand(filename)

        # Open and load the JSON file.
        with open(path, "r") as file:
            d = json.load(file)

        # Get the project id and client email.
        project_id = d["project_id"]
        client_email = d["client_email"]

        # Format the sample with json file details.
        format_sample = cls.sample.format_map(locals())
        # Convert the yaml sample to JSON.
        google_yaml = yaml.load(format_sample, Loader=yaml.SafeLoader)
        # Extract the google compute section
        google_config = google_yaml["cloudmesh"]["cloud"]

        # Update the google cloud section of cloudmesh.yaml config file.
        config = Config()
        config["cloudmesh"]["cloud"][name] = google_config
        config.save()
        banner("Result")
        pprint(config["cloudmesh"]["cloud"][name])
