import json
import os
import subprocess
import time
import uuid
from time import sleep
from pprint import pprint

import yaml
from cloudmesh.abstract.ComputeNodeABC import ComputeNodeABC
from cloudmesh.common.DateTime import DateTime
from cloudmesh.common.Printer import Printer
from cloudmesh.common.console import Console
from cloudmesh.common.util import banner
from cloudmesh.common.util import path_expand
from cloudmesh.configuration.Config import Config
from cloudmesh.management.configuration.SSHkey import SSHkey
from cloudmesh.mongo.CmDatabase import CmDatabase
from cloudmesh.secgroup.Secgroup import Secgroup, SecgroupRule
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class Provider(ComputeNodeABC):
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
                    image_project: ubuntu-os-cloud
                    project_name: cloudmesh
                    storage_bucket: cloudmesh-bucket
                    zone: us-west3-a
                    region: us-west3
                    flavor: g1-small
                    size: 10
                    resource_group: cloudmesh-group
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
                "ip_public",
                "deviceName",
                "diskSizeGb",
                "sourceImage",
                "diskType",
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
                "External IP",
                "Disk Name",
                "OS Disk Size",
                "OS Name",
                "Disk Type",
                "Provisioning State",
                "Created"]
        },
        "image": {
            "sort_keys": ["cm.name",
                          "cm.status"],
            "order": ["cm.name",
                      "id",
                      "storageLocations",
                      "diskSizeGb",
                      "creationTimestamp",
                      "status",
                      "selfLink"],
            "header": ["cm.Name",
                       "ID",
                       "Location",
                       "Disk Size",
                       "Created",
                       "Status",
                       "Link",
                       ]
        },
        "flavor": {
            "sort_keys": ["cm.name",
                          "guestCpus",
                          "memoryMb"],
            "order": ["cm.name",
                      "guestCpus",
                      "imageSpaceGb",
                      "memoryMb",
                      "maximumPersistentDisks"],
            "header": ["Name",
                       "NumberOfCores",
                       "OS_Disk_Size",
                       "Memory",
                       "Max_Data_Disk"]},
        "metadata": {
            "sort_keys": ["key"],
            "order": ["key",
                      "value"
                      ],
            "header": ["Key",
                       "Value"
                       ]
        },
        "key": {
            "sort_keys": ["name"],
            "order": ["name",
                      "type",
                      "fingerprint",
                      "comment",
                      "group"
                      ],
            "header": ["Name",
                       "Type",
                       "Fingerprint",
                       "Comment",
                       "Group"
                       ]
        },
        "secrule": {
            "sort_keys": ["name"],
            "order": ["name",
                      "protocol",
                      "ports",
                      "sourceRanges",
                      "targetTags"],
            "header": ["Name",
                       "Protocol",
                       "Ports",
                       "IP Range",
                       "Target Tags"]
        },
        "secgroup": {
            "sort_keys": ["name"],
            "order": ["name",
                      "rules",
                      "description"],
            "header": ["Name",
                       "Rules",
                       "Description"]
        }
    }

    @staticmethod
    def get_kind():
        kind = ["google"]
        return kind

    def __init__(self, name):
        cloud = name
        config = Config()["cloudmesh"]

        if cloud not in config["cloud"]:
            Console.error('Google compute configuration missing. '
                          'Please register.')

        self.cm_config = config["cloud"][cloud]["cm"]
        self.default_config = config["cloud"][cloud]["default"]
        self.credentials = config["cloud"][cloud]["credentials"]
        self.auth_config = self.credentials['auth']
        self.compute_scopes = ['https://www.googleapis.com/auth/compute',
                               'https://www.googleapis.com/auth/cloud-platform',
                               'https://www.googleapis.com/auth/compute.readonly']
        self.cloudtype = self.cm_config["kind"]
        self.cloud = name

        # verify TBD
        fields = ["project_id",
                  "client_email"]

        for field in fields:
            if self.auth_config[field] == 'TBD':
                Console.error(
                    f"The credential for Oracle cloud is incomplete. {field} "
                    "must not be TBD")

        # noinspection PyPep8Naming

    def Print(self, data, output, kind):

        if output == "table":

            order = self.output[kind]['order']
            header = self.output[kind]['header']

            print(Printer.flatwrite(data,
                                    sort_keys=["name"],
                                    order=order,
                                    header=header,
                                    output=output)
                  )
        else:
            print(Printer.write(data, output=output))

    def get_credentials(self, client_secret_file, scopes):
        """
        Method to get the credentials using the Service Account JSON file.

        :param client_secret_file: Service Account JSON File path.
        :param scopes: Scopes needed to provision.
        :return:
        """

        # Check cred file exists.
        if not os.path.exists(client_secret_file):
            Console.error(
                f"Credential file {client_secret_file} does not exists. "
                f"Check the path and try again.")
            return None

        # Authenticate using service account.
        _credentials = service_account.Credentials.from_service_account_file(
            filename=client_secret_file,
            scopes=scopes)
        return _credentials

    def _get_service(self, service_type=None, version=None, scopes=None):
        """
        Method to get service.
        """

        service_account_credentials = self.get_credentials(
            self.auth_config['json_file'],
            scopes)

        # Authenticate using service account.
        if service_account_credentials is None or service_type is None:
            Console.error('Credentials and Service Type are required.')
            raise ValueError(
                'Cannot Authenticate without Credentials or Service Type')
        else:
            compute_service = build(service_type,
                                    version or self.cm_config["version"],
                                    credentials=service_account_credentials)

        return compute_service

    def _get_compute_service(self):
        """
        Method to get compute service.
        """
        service_type = self.cm_config["service"]
        service_version = self.cm_config["version"]
        scopes = self.compute_scopes

        compute_service = self._get_service(service_type,
                                            service_version,
                                            scopes)

        return compute_service

    def _get_iam_service(self):
        """
        Method to get compute service.
        """
        service_type = 'iam'
        service_version = self.cm_config["version"]
        scopes = ['https://www.googleapis.com/auth/cloud-platform']

        iam_service = self._get_service(service_type,
                                        service_version,
                                        scopes)

        return iam_service

    def _key_dict(self, response):
        project_id = response["name"]
        commonInstanceMetadata = response["commonInstanceMetadata"]
        items = commonInstanceMetadata.get('items', [])
        id = response["id"]
        selfLink = response["selfLink"]

        keys = []

        for item in items:
            key = item['key']
            if key == 'ssh-keys':
                value = item['value']

                for line in value.splitlines():
                    key_dict = {}

                    if line is None or line.strip() == '':
                        # if line is empty, then dont process.
                        continue

                    key_items = line.split()

                    name_items = key_items[0].split(":")
                    key_dict["name"] = name_items[0]
                    key_dict["type"] = name_items[1]

                    key_dict["key"] = key_items[1]

                    key_dict["comment"] = key_items[2]

                    # Join format and key.
                    key_dict["public_key"] = f"{key_dict['type']} " \
                                             f"{key_dict['key']} " \
                                             f"{key_dict['comment']}"

                    key_dict["private_key"] = None

                    fingerPrint = SSHkey._fingerprint(key_dict["public_key"])

                    key_dict["fingerprint"] = fingerPrint

                    key_dict["group"] = self.kind

                    if len(key_items) > 3:
                        user_item = json.loads(key_items[3])
                        key_dict["user_id"] = user_item["userName"]
                        key_dict["expireOn"] = user_item["expireOn"]
                    else:
                        key_dict["user_id"] = None
                        key_dict["expireOn"] = None

                    key_dict["location"] = {"cloud": self.kind,
                                            "region_name": None,
                                            "zone": None,
                                            "project": {
                                                "id": project_id,
                                                "name": self.kind,
                                                "domain_id": id,
                                                "domain_name": selfLink
                                            }
                                            }

                    keys.append(key_dict)

        return keys

    def _process_status(self, instance):
        instance_dict = self._process_instance(instance)
        status_dict = {"name": instance_dict["cm.name"],
                       "kind": instance_dict["cm.kind"],
                       "status": instance_dict["status"],
                       "id": instance_dict["id"], "zone": instance_dict["zone"]}

        return status_dict

    def _process_instance(self, instance):
        """
        converts the instance json to dict.

        :param instance: JSON with instance details
        :return:
        """
        instance_dict = {}
        ins_zone = instance["zone"]
        instance_dict["zone"] = ins_zone[
                                ins_zone.index("zones/") + 6:len(ins_zone)]
        instance_dict["name"] = instance["name"]
        instance_dict["cloud"] = self.kind
        instance_dict["status"] = instance["status"]
        instance_dict["type"] = instance["cpuPlatform"]
        instance_dict["created"] = instance["creationTimestamp"]
        instance_dict["id"] = instance["id"]
        instance_dict["kind"] = instance["kind"]
        instance_dict["fingerprint"] = instance["fingerprint"]
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
        instance_dict["diskType"] = disk["type"]
        instance_dict["mode"] = disk["mode"]
        instance_dict["modified"] = str(DateTime.now())

        # Metadata
        instance_metadata = instance.get("metadata", {})
        instance_dict["metadata"] = instance_metadata.get('items', [])

        # firewall tags.
        instance_dict["tags"] = instance["tags"]

        # Network access.
        network_config = instance["networkInterfaces"]

        if network_config:
            network_config = network_config[0]
            instance_dict["network_fingerprint"] = network_config["fingerprint"]
            instance_dict["networkIP"] = network_config["networkIP"]
            access_config = network_config["accessConfigs"]
            access_config = access_config[0]
            if "natIP" in access_config:
                external_ip = access_config["natIP"]
            else:
                external_ip = None

            instance_dict["ip_public"] = external_ip

        return instance_dict

    def update_dict(self, elements, kind=None):
        """
        adds a cloudmesh cm dict to each dict in the list elements.

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

            if kind == 'status':
                entry["cm"]["updated"] = str(DateTime.now())
                if 'status' in entry:
                    entry["cm"]["status"] = str(entry["status"])

            elif kind == 'vm':

                entry["cm"]["updated"] = str(DateTime.now())

                if "created" in entry:
                    entry["cm"]["created"] = DateTime.utc(entry["created"])
                    # del entry["created_at"]
                    if 'status' in entry:
                        entry["cm"]["status"] = str(entry["status"])
                else:
                    entry["cm"]["created"] = entry["modified"]

            elif kind == 'flavor':

                entry["cm"]["created"] = entry["updated"] = str(DateTime.now())

            elif kind == 'image':

                entry["cm"]["created"] = entry["updated"] = str(DateTime.now())

            elif kind == 'key':
                entry["cm"]["created"] = entry["updated"] = str(DateTime.now())

            # elif kind == 'secgroup':
            #    pass

            d.append(entry)

        # VERBOSE(d)

        return d

    def _format_aggregate_list(self, instance_list):
        """
        formats the instance list to flat dict format.

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

    def _format_zone_list(self, instance_list):
        """
        formats the instance list to flat dict format.

        :param instance_list:
        :return: dict
        """
        result = []
        if instance_list is not None:
            if "items" in instance_list:
                items = instance_list["items"]
                for item in items:
                    result.append(self._process_instance(item))
        return result

    def _wait_for_operation(self, compute_service, operation, project,
                            zone=None, name=None):

        operation_name = operation["name"]
        operation_type = operation["operationType"]
        Console.info(
            f'Waiting for {operation_type} operation to finish : {name}')

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

                if 'error' in result or 'httpErrorMessage' in result:
                    Console.error("Error in operation")
                    raise Exception(result['error']['errors'])

                if result['status'] == 'DONE':
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

    def start(self, name=None, **kwargs):
        """
        start a node

        :param name: the unique node name
        :return:  The dict representing the node
        """
        result = {}
        compute_service = self._get_compute_service()
        _operation = None

        if name is None:
            Console.error("Instance name is required to start.")
            return result

        try:

            project_id = kwargs.get('project_id',
                                    self.auth_config["project_id"])
            zone = kwargs.get('zone', self.default_config["zone"])

            _operation = compute_service.instances()\
                                        .start(project=project_id,
                                               zone=zone,
                                               instance=name).execute()

            self._wait_for_operation(compute_service,
                                     _operation,
                                     project_id,
                                     zone,
                                     name)

        except Exception as se:
            if type(se) == HttpError:
                Console.error(
                    f'Unable to start instance {name}. '
                    f'Reason: {se._get_reason()}')
            else:
                Console.error(f'Unable to start instance {name}.')
        else:
            # Get the instance details to update DB.
            result = self._info(name, displayType="vm")

        return result

    def stop(self, name=None, **kwargs):
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

            project_id = kwargs.get('project_id',
                                    self.auth_config["project_id"])
            zone = kwargs.get('zone', self.default_config["zone"])

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
            result = self._info(name, displayType="vm")

        except Exception as se:
            if type(se) == HttpError:
                Console.error(
                    f'Unable to stop instance {name}. '
                    f'Reason: {se._get_reason()}')
            else:
                Console.error(f'Unable to stop instance {name}.')

        return result

    def _raw_instance_info(self, name, compute_service=None, **kwargs):
        """
        gets the information of a node with a given name

        :param name:
        :param displayType:
        :return:
        """

        project_id = kwargs.get('project_id', self.auth_config["project_id"])
        zone = kwargs.get('zone', self.default_config["zone"])

        if compute_service is None:
            compute_service = self._get_compute_service()

        # Get the instance details to update DB.
        result = compute_service.instances().get(project=project_id,
                                                 zone=zone,
                                                 instance=name).execute()

        return result

    def _info(self, name, displayType, compute_service=None):
        """
        gets the information of a node with a given name

        :param name:
        :param displayType:
        :return:
        """

        result = self._raw_instance_info(name, compute_service)

        if not displayType:
            displayType = "vm"

        if displayType == 'status':
            result = self._process_status(result)
        elif displayType == "vm":
            result = self._process_instance(result)

        result = self.update_dict(result, kind=displayType)

        return result

    def info(self, name=None, **kwargs):
        """
        gets the information of a node with a given name

        :param name:
        :return: The dict representing the node including updated status
        """

        result = None
        if name is None:
            Console.error("Instance name is required to start.")
            return

        display_kind = kwargs.get('kind', "vm")

        try:
            result = self._info(name, displayType=display_kind)
        except Exception as se:
            if type(se) == HttpError:
                Console.error(
                    f'Unable to get instance {name} info. '
                    f'Reason: {se._get_reason()}')
            else:
                Console.error(f'Unable to get info of instance {name}.')

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

        project_id = kwargs.get('project_id', self.auth_config["project_id"])
        zone = kwargs.get('zone', None)

        try:
            compute_service = self._get_compute_service()

            if zone is None:
                # Get aggregate list of all zones.
                aggregatedList = compute_service.instances().aggregatedList(
                    project=project_id,
                    orderBy="name").execute()
                result = self._format_aggregate_list(aggregatedList)
            else:
                # Get instance list of given zone.
                zoneList = compute_service.instances().list(
                    project=project_id,
                    zone=zone,
                    orderBy="name").execute()
                result = self._format_zone_list(zoneList)

            result = self.update_dict(result, kind="vm")

        except Exception as se:
            print(se)

        return result

    def resume(self, name=None):
        """
        resume the named node

        :param name: the name of the node
        :return: the dict of the node
        """

        compute_service = self._get_compute_service()

        try:
            request = compute_service.instances() \
                .reset(project=self.auth_config["project_id"],
                       zone=self.auth_config["zone"],
                       instance=name)

            response = request.execute()

        except Exception as se:
            print(se)
            result = {}
        else:
            result = response

        return result

    def destroy(self, name=None, **kwargs):
        """
        destroys the node

        :param name: the name of the node
        :return: the dict of the node
        """

        vm = None
        compute_service = self._get_compute_service()
        _operation = None

        if name is None:
            return
        try:

            vm = self.info(name=name)

            if type(vm) == list:
                vm = vm[0]

            project_id = kwargs.get('project_id',
                                    self.auth_config["project_id"])
            zone = kwargs.get('zone', self.default_config["zone"])

            _operation = compute_service.instances().delete(
                project=project_id,
                zone=zone,
                instance=name).execute()

            self._wait_for_operation(compute_service,
                                     _operation,
                                     project_id,
                                     zone,
                                     name)

            Console.ok(f"{name} deleted successfully.")

        except Exception as se:
            if type(se) == HttpError:
                Console.error(
                    f'Unable to delete instance {name}. '
                    f'Reason: {se._get_reason()}')
            else:
                Console.error(f'Unable to delete instance {name}.')
        else:
            # Set status to Delete for DB entry
            vm["status"] = "DELETED"

        return vm

    def _get_compute_config(self, vm_name, project_id, zone, machine_type,
                            disk_image, storage_bucket, startup_script,
                            diskSize, secgroup):

        project_zone = f"projects/{project_id}/zones/{zone}"

        secrules = []

        if secgroup:
            # Get required sec group.
            grp = self._list_local_secgroups(secgroup)
            if grp:
                for rule in grp[0]['rules']:
                    secrules.append(f'cm-{secgroup}-{rule}')

        compute_config = {
            "kind": "compute#instance",
            "name": vm_name,
            "zone": project_zone,
            "machineType": f"{project_zone}/machineTypes/{machine_type}",
            "displayDevice": {
                "enableDisplay": False
            },
            "metadata": {
                "kind": "compute#metadata",
                "items": [
                    {
                        "key": "startup-script",
                        "value": startup_script
                    },
                    {
                        'key': 'bucket',
                        'value': storage_bucket
                    }
                ]
            },
            "tags": {
                "items": secrules,
            },
            "disks": [
                {
                    "kind": "compute#attachedDisk",
                    "type": "PERSISTENT",
                    "boot": True,
                    "mode": "READ_WRITE",
                    "autoDelete": True,
                    "deviceName": f"{vm_name}-disk",
                    "initializeParams": {
                        "sourceImage": disk_image,
                        "diskType": f"{project_zone}/diskTypes/pd-standard",
                        "diskSizeGb": diskSize
                    }
                }
            ],
            "canIpForward": False,
            "networkInterfaces": [
                {
                    "kind": "compute#networkInterface",
                    "accessConfigs": [
                        {
                            "kind": "compute#accessConfig",
                            "name": "External NAT",
                            "type": "ONE_TO_ONE_NAT",
                            "networkTier": "STANDARD"
                        }
                    ]
                }
            ],
            "description": f"{vm_name} created using cloudmesh.",
            "labels": {
                "project_id": "cloudmesh"
            },
            "scheduling": {
                "onHostMaintenance": "TERMINATE",
                "automaticRestart": False
            },
            "deletionProtection": False,
            "reservationAffinity": {
                "consumeReservationType": "ANY_RESERVATION"
            },
            "serviceAccounts": [
                {
                    "email": self.auth_config['client_email'],
                    "scopes": [
                        "https://www.googleapis.com/auth/devstorage.read_only",
                        "https://www.googleapis.com/auth/logging.write",
                        "https://www.googleapis.com/auth/monitoring.write",
                        "https://www.googleapis.com/auth/servicecontrol",
                        "https://www.googleapis.com/auth/service.management.readonly",
                        "https://www.googleapis.com/auth/trace.append"
                    ]
                }
            ],
            "shieldedInstanceConfig": {
                "enableSecureBoot": False,
                "enableVtpm": True,
                "enableIntegrityMonitoring": True
            }
        }

        return compute_config

    # TODO: Change params to dict or kwargs.
    def _create_instance(self, compute_service, project, zone, name, bucket,
                         disk_image, machineType, startup_script, diskSize,
                         secgroup):

        """
        create a VM instance for given name.

        :param compute_service:
        :param project:
        :param zone:
        :param name:
        :param bucket:
        :param disk_image:
        :param machineType:
        :param startup_script:
        :param diskSize:
        :param secgroup:
        :return:
        """

        result = {}

        if startup_script:
            startup_script = open(startup_script, 'r').read()

        compute_config = self._get_compute_config(name,
                                                  project,
                                                  zone,
                                                  machineType,
                                                  disk_image,
                                                  bucket,
                                                  startup_script,
                                                  diskSize,
                                                  secgroup)

        try:
            # Invoke compute insert comment to create a new instance.
            compute_operation = compute_service.instances() \
                .insert(project=project,
                        zone=zone,
                        body=compute_config) \
                .execute()

            opearation_result = self._wait_for_operation(compute_service,
                                                         compute_operation,
                                                         project, zone, name)

            if opearation_result["status"] == 'DONE':
                Console.ok(f"Instance {name} created successfully.")
            else:
                raise ValueError(
                    f"Instance {name} creation operation did not finish.")

        except Exception as de:
            if type(de) is HttpError:
                Console.error(
                    f"Error creating instance: {name} - {de._get_reason()}")
            else:
                Console.error(f"Error creating instance: {name} - {de}")

            raise ValueError(f"Creating of instance: {name} failed.")

        else:
            vm_info = self._info(name,
                                 displayType="vm",
                                 compute_service=compute_service)

            if type(vm_info) is list:
                result = vm_info[0]
            else:
                result = vm_info

        return result

    def create(self,
               name=None,
               image=None,
               size=10,
               timeout=360,
               group=None,
               **kwargs):
        """
        creates a named node.

        :param name: the name of the node
        :param image: the image used
        :param size: the size of the image
        :param timeout: a timeout in seconds that is invoked in case the
               image does not boot.
               The default is set to 3 minutes.
        :param group: a list of groups the vm belongs to
        :param kwargs: additional arguments passed along at time of boot
        :return:
        """

        resource_group = group or self.default_config['resource_group']
        secgroup = kwargs.get('secgroup', 'default')

        bucket = kwargs.get('storage_bucket',
                            self.default_config['storage_bucket'])

        name = name or 'vm1'
        compute_service = self._get_compute_service()
        project_id = self.auth_config['project_id']
        zone = kwargs.get('zone', self.default_config['zone'])

        machineType = kwargs.get('flavor', self.default_config['flavor'])

        if machineType is None:
            machineType = 'g1-small'

        startup_script = kwargs.get('startup_script', None)

        # Get the image link using the name of the image.
        os_image = self.image(image)

        if type(os_image) == list:
            os_image = os_image[0]

        disk_image = os_image['selfLink']

        result = self._create_instance(compute_service, project_id, zone, name,
                                       bucket, disk_image, machineType,
                                       startup_script, diskSize=size,
                                       secgroup=secgroup)

        return result

    def set_server_metadata(self, name, **keys):
        """
        sets the metadata for the server

        :param name: name of the VM
        :param keys: dict of metadata that needs to be set on VM.
        :return:
        """

        metadata = self._get_instance_metadata(name)

        metadata_items = metadata.get('items', [])

        for key, value in keys.items():
            # banner(f"{key}={value}")
            metadata_items.append({"key": key, "value": value})

        metadata['items'] = metadata_items

        project_id = self.auth_config["project_id"]
        zone = self.default_config["zone"]

        operation_result = self._update_metadata(project_id,
                                                 zone,
                                                 name,
                                                 metadata)

        if operation_result and operation_result.get('status') == 'DONE':
            Console.ok(f"Metadata keys added to instance {name}")

        return metadata_items

    def get_server_metadata(self, name):
        """
        gets the metadata for the server

        :param name: name of the fm
        :return:
        """
        metadata = self._get_instance_metadata(name)
        metadata_items = metadata.get('items')
        return metadata_items

    def _update_metadata(self, project_id, zone, name, instance_metadata):
        """
        adds/updates/deletes the instance metadata

        :param project_id:
        :param zone:
        :param name:
        :param instance_metadata:
        :return:
        """

        result = None
        compute_service = self._get_compute_service()
        _operation = None

        if name is None:
            return
        try:

            requestId = str(uuid.uuid1())

            _operation = compute_service.instances().setMetadata(
                project=project_id,
                zone=zone,
                instance=name,
                body=instance_metadata,
                requestId=requestId).execute()

            result = self._wait_for_operation(compute_service,
                                              _operation,
                                              project_id,
                                              zone,
                                              name)

        except Exception as se:
            if type(se) == HttpError:
                Console.error(
                    f'Unable to update metadata on instance {name}. '
                    f'Reason: {se._get_reason()}')
            else:
                Console.error(f'Unable to update metadata on instance {name}.')

        return result

    def delete_server_metadata(self, name, key):
        """
        Gets the metadata for the server

        :param name: name of the vm
        :param key:
        :return:
        """
        """
        
        :param name: name of the fm
        
        :return:
        """

        metadata = self._get_instance_metadata(name)

        metadata_items = metadata.get('items')

        key_exists = False

        for item in metadata_items:
            if item['key'] == key:
                metadata_items.remove(item)
                key_exists = True
                break

        if not key_exists:
            Console.error(f"Metadata key {key} not found in instance {name}")
            return metadata_items

        metadata['items'] = metadata_items

        project_id = self.auth_config["project_id"]
        zone = self.default_config["zone"]

        operation_result = self._update_metadata(project_id, zone, name,
                                                 metadata)

        if operation_result and operation_result.get('status') == 'DONE':
            Console.ok(f"Metadata key {key} deleted from instance {name}")

        return metadata_items

    def rename(self, name=None, destination=None):
        """
        rename a node

        :param destination:
        :param name: the current name
        :return: the dict with the new name
        """
        # if destination is None, increase the name counter and use the new name
        Console.error("Google cloud does not allow renaming of VM")
        raise NotImplementedError

    def _get_project_metadata(self, project_id):
        """
        gets a list of keys from google project.

        :param project_id: Project Id to get info for.
        :return:
        """

        compute_service = self._get_compute_service()

        response = compute_service.projects().get(project=project_id).execute()

        return response

    def _get_instance_metadata(self, name):
        """
        get a list of keys from google project.

        :param project_id: Project Id to get info for.
        :return:
        """
        metadata = {}

        try:
            info = self._raw_instance_info(name)

            metadata = info.get('metadata')

        except:
            Console.error(f"Instance with name {name} not found.")

        return metadata

    def _get_keys(self, cloud):
        """
        get s keys on google cloud from DB.

        :param cloud:
        :return:
        """
        db = CmDatabase()
        db_keys = db.find(collection=f"{cloud}-key")

        if db_keys is None or len(db_keys) < 1:
            db_keys = self.keys()

        return db_keys

    def _key_already_exists(self, cloud, name, public_key):
        """
        checks if the key with name already exists.

        :param name: Name of the key to be added and checked.
        :return:
        """
        key_found = None

        db_keys = self._get_keys(cloud)

        fingerprint = SSHkey._fingerprint(public_key)

        for key in db_keys:
            if key["fingerprint"] == fingerprint:
                key_found = key
                break

        return key_found

    def keys(self):
        """
        Lists the keys on the cloud

        :return: dict
        """

        # Get the project id from auth config.
        project_id = self.auth_config['project_id']

        proj_metadata = self._get_project_metadata(project_id)

        # Generate a simple dict from response.
        keys = self._key_dict(proj_metadata)

        # Add Cm entry to dict.
        cm_keys = self.update_dict(keys, "key")

        return cm_keys

    def key_upload(self, key=None):
        """
        uploads the key specified in the yaml configuration to the cloud

        :param key:
        :return:
        """

        name = key["name"]

        # Get the project id from auth config.
        project_id = self.auth_config['project_id']

        try:
            requestId = str(uuid.uuid1())

            proj_metadata = self._get_project_metadata(project_id)

            commonInstanceMetadata = proj_metadata['commonInstanceMetadata']
            items = commonInstanceMetadata.get('items') or []

            # Compuse new key.
            new_key = f"{key['name']}:{key['public_key']}"

            if 'user_id' in key:
                user_info = {"userName": key["user_id"],
                             "expireOn": key["expireOn"]}

                new_key = f"{new_key} {user_info} \n"

            keys_exists = False

            for item in items:
                if item['key'] == 'ssh-keys':
                    currVal = item["value"]
                    newVal = f"{currVal}\n{new_key}"
                    item["value"] = newVal
                    keys_exists = True
                    break

            if not keys_exists:
                # If keys does not exists, then append.
                items.append({
                    "key": "ssh-keys",
                    "value": new_key
                })

            commonInstanceMetadata['items'] = items

            compute_service = self._get_compute_service()

            _oper = compute_service.projects().setCommonInstanceMetadata(
                project=project_id,
                body=commonInstanceMetadata,
                requestId=requestId).execute()

            self._wait_for_operation(compute_service, _oper,
                                     project_id,
                                     name=name)

        except:
            raise ValueError(f"Error uploading key : {name}")

        return key

    def key_delete(self, name=None):
        """
        deletes the key with the given name

        :param name: The name of the key
        :return:
        """

        # Get the project id from auth config.
        project_id = self.auth_config['project_id']

        key = self._get_keys(self.cloud)

        try:
            requestId = str(uuid.uuid1())

            proj_metadata = self._get_project_metadata(project_id)

            commonInstanceMetadata = proj_metadata['commonInstanceMetadata']
            keys = self._key_dict(proj_metadata)

            key_found = False
            for key in keys:
                if key['name'] == name:
                    keys.remove(key)
                    key_found = True
                    break

            if not key_found:
                Console.error(f"Key {name} not found.")
                raise ValueError(f"Key {name} not found.")

            items = commonInstanceMetadata.get('items') or []

            newVal = None

            for item in items:
                if item['key'] == 'ssh-keys':
                    for key in keys:
                        # Compuse new key list
                        new_key = f"{key['name']}:{key['public_key']}"

                        if key.get('user_id') is not None:
                            user_info = {"userName": key["user_id"],
                                         "expireOn": key["expireOn"]}

                            new_key = f"{new_key} {user_info}\n"

                        if newVal is None:
                            newVal = new_key
                        else:
                            newVal = f"{newVal}\n{new_key}"

                    if newVal:
                        item["value"] = newVal
                    else:
                        # No more keys, remove ssh-keys dict from list.
                        items.remove(item)

            commonInstanceMetadata['items'] = items

            # Update project metadata to delete the key.
            compute_service = self._get_compute_service()

            _oper = compute_service.projects().setCommonInstanceMetadata(
                project=project_id,
                body=commonInstanceMetadata,
                requestId=requestId).execute()

            self._wait_for_operation(compute_service, _oper,
                                     project_id,
                                     name=name)

        except:
            raise ValueError(f"Error deleting key : {name}")
        else:
            Console.ok(f"Key {name} is deleted.")

        return key

    def images(self, **kwargs):
        """
        Lists the images on the cloud

        :return: dict
        """
        result = None

        # Get the images for the image project.
        image_project = kwargs.get('image_project',
                                   self.default_config["image_project"])

        image_zone = kwargs.get('zone', self.default_config["zone"])

        try:
            compute_service = self._get_compute_service()

            # Get list of Custom images related to image project.
            image_list = []
            _next_url = None

            # Iterate to get images till nextToken is None.
            while True:
                # Make the first call.
                image_request = compute_service.images().list(
                    project=image_project,
                    orderBy="name", pageToken=_next_url)

                image_response = image_request.execute()

                if "items" in image_response:
                    list_items = image_response["items"]
                    image_list.extend(list_items)

                if not image_response and "nextPageToken" in image_response:
                    _next_url = image_response["nextPageToken"]
                else:
                    break

            result = self.update_dict(image_list, kind="image")

        except Exception as e:
            print(f'Error when getting images {e}')

        return result

    def image(self, name=None, **kwargs):
        """
        Gets the image with a given name

        :param name: The name of the image
        :return: the dict of the image
        """

        result = None

        # Check DB:
        cm = CmDatabase()

        query = {"name": {'$regex': f".*{name}.*"}}
        entries = cm.find(collection="google-image", query=query)

        for entry in entries:
            if name in entry["name"]:
                result = entry
                if "deprecated" not in entry:
                    break

        cm.close_client()

        # If not found in Db, get it from provider.
        if result is None:
            # Get the images for the image project.
            image_project = kwargs.get('image_project',
                                       self.default_config["image_project"])

            image_name = name or self.default_config["image"]

            compute_service = self._get_compute_service()

            image = compute_service.images().getFromFamily(
                project=image_project,
                family=image_name).execute()

            result = self.update_dict(image, kind="image")

        return result

    def flavor(self, name, **kwargs):
        """
        Gets the flavor with a given name

        :param name: The name of the flavor
        :return: The dict of the flavor
        """
        comput_servce = self._get_compute_service()
        project_id = kwargs.get('project_id', self.auth_config['project_id'])
        zone = kwargs.get('zone', self.default_config['zone'])
        flavor = self._get_flavor(comput_servce, project_id, zone, name)

        return flavor

    def _get_flavor(self, compute_service, project_id, zone, name):
        # Get the flavor for the project_id.
        flavor = None
        try:
            flavor = compute_service.machineTypes()\
                                    .get(project=project_id,
                                         zone=zone,
                                         machineType=name).execute()
        except Exception as e:
            print(f'Error in get_flavors {e}')
        flavor = self.update_dict(flavor, kind='flavor')

        return flavor

    def flavors(self, **kwargs):
        """
        Lists the flavors on the cloud

        :return: dict of flavors
        """
        comput_servce = self._get_compute_service()
        project_id = kwargs.get('project_id', self.auth_config['project_id'])
        zone = kwargs.get('zone', self.default_config['zone'])

        return self._get_flavors(comput_servce, project_id, zone)

    def _get_flavors(self, compute_service, project_id, zone):
        source_disk_flavor = None
        # Get the flavors for the image project.
        try:
            # Get list of images related to image project.
            flavor_response = compute_service.machineTypes() \
                .list(project=project_id,
                      zone=zone).execute()
            # Extract the items.
            source_disk_flavor = flavor_response['items']
        except Exception as e:
            print(f'Error in get_flavors {e}')
        source_disk_flavor = self.update_dict(source_disk_flavor, kind='flavor')

        return source_disk_flavor

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

    def _list_local_secgroups(self, name=None):
        """
        Method to get the local sec group list.
        :param name:
        :return:
        """
        secgroup = Secgroup()

        grp_list = secgroup.list(name)

        secrule = SecgroupRule()

        for item in grp_list:
            security_group_rules = []
            # Extract rules.
            for rulename in item['rules']:
                rule_detail = secrule.find(name=rulename)
                security_group_rules.append(rule_detail[0])

            item["security_group_rules"] = security_group_rules

        return grp_list

    def list_secgroups(self, name=None):
        """
        List the named security group

        :param name: The name of the group, if None all will be returned
        :return:
        """

        firewall_list = []

        try:
            project_id = self.auth_config['project_id']

            compute_service = self._get_compute_service()

            result = compute_service.firewalls() \
                .list(project=project_id,
                      orderBy="name") \
                .execute()

            if "items" in result:
                rules = result["items"]
                for rule in rules:
                    if name:
                        if f"cm-{name}-" in rule['name']:
                            firewall_list.append(rule)
                    else:
                        if f"cm-" in rule['name']:
                            firewall_list.append(rule)

            added = []
            firewalls = {}

            for item in firewall_list:
                rule = item["name"]
                names = rule.split('-')
                names_len = len(names)

                if names_len < 3:
                    continue
                elif names_len == 3:
                    sec_group_name = name or names[1]
                else:
                    if name:
                        sec_group_name = name
                    else:
                        sec_group_name = names[1]
                        for i in range(2, names_len - 1):
                            sec_group_name = sec_group_name + '-' + names[i]

                if sec_group_name in added:
                    firewalls[sec_group_name]["security_group_rules"].append(
                        item)
                    firewalls[sec_group_name]["rules"].append(
                        names[names_len - 1])
                else:
                    firewalls[sec_group_name] = {
                        "name": sec_group_name,
                        "description": item['description'].split('-')[1],
                        "rules": [names[names_len - 1]],
                        "security_group_rules": [item]
                    }
                    added.append(sec_group_name)

        except Exception as e:
            Console.error(f"Error : {e}")
            # raise ValueError(f"Error  : {e}")
            firewalls = {}

        return list(firewalls.values())

    def list_secgroup_rules(self, name=None):
        """
        List the named security group

        :param name: The name of the group, if None all will be returned
        :return:
        """

        firewall_list = self.list_secgroups(name)
        result = []

        for group in firewall_list:
            for rule in group['security_group_rules']:
                # Exctract rule name from cm={groupname}={rulename} format.
                rule_names = rule['name'].split("-")
                rule['name'] = rule_names[len(rule_names) - 1]
                allowed = rule['allowed'][0]
                rule['protocol'] = allowed['IPProtocol']
                if 'ports' in allowed:
                    rule['ports'] = allowed['ports']

                result.append(rule)

        return result

    def upload_secgroup(self, name=None):
        """
        Method to upload rules from the given sec-group to google cloud.
        :param name: Name of the security group.
        :return:
        """
        if not name:
            Console.error('Sec Group Name is required to upload Rules')
            raise ValueError('Sec Group Name is required to upload Rules')

        # Get required sec group.
        grp = self._list_local_secgroups(name)

        if grp:
            grp = grp[0]
        else:
            Console.warning(f"No sec group found with name {name}")
            return

        security_group_rules = grp['security_group_rules']

        project_id = self.auth_config['project_id']

        compute_service = self._get_compute_service()

        for rule in security_group_rules:
            firewall_name = f"cm-{name}-{rule['name']}"

            firewall = {
                "kind": "compute#firewall",
                "name": firewall_name,
                "network": f"projects/{project_id}/global/networks/default",
                "direction": "INGRESS",
                "priority": 1000,
                "description": f"{rule['name']} - {grp['description']}",
                "targetTags": [
                    firewall_name
                ],
                "allowed": [
                    {
                        "IPProtocol": rule['protocol'],
                        "ports": [
                            rule["ports"].replace(':', '-')
                        ]
                    }
                ],
                "sourceRanges": [
                    rule["ip_range"]
                ]
            }

            if not firewall["allowed"][0]["ports"][0]:
                del firewall["allowed"][0]["ports"]

            requestId = str(uuid.uuid1())

            try:
                _oper = compute_service.firewalls() \
                    .insert(project=project_id,
                            body=firewall,
                            requestId=requestId).execute()
            except Exception as se:
                if type(se) == HttpError:
                    Console.error(
                        f'Error uploading rule: Reason: {se._get_reason()}')
                else:
                    Console.error(f'Error uploading sec group {name}.')
            else:
                if _oper:
                    self._wait_for_operation(compute_service, _oper,
                                             project_id,
                                             name=firewall_name)

        return None

    def add_secgroup(self, name=None, description=None):
        raise NotImplementedError

    def add_secgroup_rule(self,
                          name=None,  # group name
                          port=None,
                          protocol=None,
                          ip_range=None):
        raise NotImplementedError

    def remove_secgroup(self, name=None):
        """
        Method to remove sec group google cloud. On GCP it will remove all rules
        with name format cm-{name}- from the project.
        :param name: Name of the secgroup
        :return:
        """

        if not name:
            Console.error('Sec Group Name is required to delete Rules')
            raise ValueError('Sec Group Name is required to delete Rules')

        rules = self.list_secgroup_rules(name)

        project_id = self.auth_config['project_id']

        compute_service = self._get_compute_service()

        name_format = f"cm-{name}-"

        for rule in rules:
            requestId = str(uuid.uuid1())
            firewall_name = rule['targetTags'][0]
            if name_format in firewall_name:
                try:
                    _oper = compute_service.firewalls() \
                        .delete(project=project_id,
                                firewall=firewall_name,
                                requestId=requestId).execute()
                except Exception as se:
                    if type(se) == HttpError:
                        Console.error(
                            f'Error deleting rule: Reason: {se._get_reason()}')
                    else:
                        Console.error(f'Unable to delete instance {name}.')
                else:
                    if _oper:
                        self._wait_for_operation(compute_service, _oper,
                                                 project_id,
                                                 name=firewall_name)
        return ""

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
        name = vm['name']
        if interval is None:
            # if interval is too low, OS will block your ip (I think)
            interval = 10
        if timeout is None:
            timeout = 60
        Console.info(
            f"waiting for instance {name} to be reachable: Interval: "
            f"{interval}, Timeout: {timeout}")
        timer = 0
        while timer < timeout:
            sleep(interval)
            timer += interval
            try:
                r = self.list()
                r = self.ssh(vm=vm, command='echo IAmReady').strip()
                if 'IAmReady' in r:
                    return True
            except:
                # If the error is not connection refused, then break.
                Console.error(f"Unable to ssh to VM {name}")
                break

        return False

    def console(self, vm=None):
        """
        gets the output from the console

        :param vm: name of the VM
        :return:
        """
        raise NotImplementedError

    def log(self, vm=None):
        raise NotImplementedError

    def ssh(self, vm=None, command=None):

        ip = vm['ip_public']
        result = None

        if ip is None:
            Console.error("Public IP for VM not found.")
            return result
        else:
            location = ip

        if command is None:
            command = ""

        cmd = "ssh " \
              "-o StrictHostKeyChecking=no " \
              "-o UserKnownHostsFile=/dev/null " \
              f" {location} {command}"

        cmd = cmd.strip()

        if command == "":
            os.system(cmd)
        else:
            ssh = subprocess.Popen(cmd,
                                   shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)

            result = ssh.stdout.read().decode("utf-8")

            if not result:
                error = ssh.stderr.readlines()
                for line in error:
                    Console.error(line, prefix=False)

        return result

    @staticmethod
    def json_to_yaml(cls, name, filename="~/.cloudmesh/security/google.json"):
        """
        Given a json file downloaded from google, copies the content into the
        cloudmesh yaml file, while overwriting or creating a new
        compute provider

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
