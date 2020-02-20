from abc import ABCMeta, abstractmethod
from cloudmesh.configuration.Config import Config
from cloudmesh.abstractclass.ComputeNodeABC import ComputeNodeABC
from cloudmesh.provider import ComputeProviderPlugin


# noinspection PyUnusedLocal
class Provider(ComputeNodeABC, ComputeProviderPlugin):
    kind = 'google'

    #
    # TODO: I just copied this from Azure, cahnge for Google
    #

    sample = """
            cloudmesh:
              cloud:
                {name}:
                  cm:
                    active: true
                    heading: {name}
                    host: TBD
                    label: {name}
                    kind: azure
                    version: latest
                    service: compute
                  default:
                    image: Canonical:UbuntuServer:16.04.0-LTS:latest
                    size: Basic_A0
                    resource_group: cloudmesh
                    storage_account: cmdrive
                    network: cmnetwork
                    subnet: cmsubnet
                    blob_container: vhds
                    AZURE_VM_IP_CONFIG: cloudmesh-ip-config
                    AZURE_VM_NIC: cloudmesh-nic
                    AZURE_VM_DISK_NAME: cloudmesh-os-disk
                    AZURE_VM_USER: TBD
                    AZURE_VM_PASSWORD: TBD
                    AZURE_VM_NAME: cloudmeshVM
                  credentials:
                    AZURE_TENANT_ID: {tenantid}
                    AZURE_SUBSCRIPTION_ID: {subscriptionid}
                    AZURE_APPLICATION_ID: {applicationid}
                    AZURE_SECRET_KEY: {secretkey}
                    AZURE_REGION: eastus
            """

    #
    # TODO: I just copied this from Azure, cahnge for Google
    #

    vm_state = [
        'ACTIVE',
        'BUILDING',
        'DELETED',
        'ERROR',
        'HARD_REBOOT',
        'PASSWORD',
        'PAUSED',
        'REBOOT',
        'REBUILD',
        'RESCUED',
        'RESIZED',
        'REVERT_RESIZE',
        'SHUTOFF',
        'SOFT_DELETED',
        'STOPPED',
        'SUSPENDED',
        'UNKNOWN',
        'VERIFY_RESIZE'
    ]

    #
    # TODO: I just copied this from Azure, cahnge for Google
    #

    output = {
        "status": {
            "sort_keys": ["cm.name"],
            "order": ["cm.name",
                      "cm.cloud",
                      "vm_state",
                      "status",
                      "task_state"],
            "header": ["Name",
                       "Cloud",
                       "State",
                       "Status",
                       "Task"]
        },
        "vm": {
            "sort_keys": ["cm.name"],
            "order": [
                "cm.name",
                "cm.cloud",
                "id",
                "type",
                "location",
                "hardware_profile.vm_size",
                "storage_profile.image_reference.offer",
                "storage_profile.image_reference.sku",
                "storage_profile.os_disk.disk_size_gb",
                "provisioning_state",
                "vm_id",
                "cm.kind"],
            "header": [
                "Name",
                "Cloud",
                "Id",
                "Type",
                "Location",
                "VM Size",
                "OS Name",
                "OS Version",
                "OS Disk Size",
                "Provisioning State",
                "VM ID",
                "Kind"]
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
            "header": ["Name",
                       "Location",
                       "Publisher",
                       "Plan Name",
                       "Product",
                       "Operating System",
                       ]
        },
        "flavor": {
            "sort_keys": ["name",
                          "number_of_cores",
                          "os_disk_size_in_mb"],
            "order": ["name",
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

    def __init__(self, cloud, path):
        config = Config(config_path=path)["cloudmesh"]
        self.cm = config["cloud"][cloud]["cm"]
        self.default = config["cloud"][cloud]["default"]
        self.credentials = config["cloud"][cloud]["credentials"]
        self.group = config["default"]["group"]
        self.experiment = config["default"]["experiment"]

    def start(self, name=None):
        """
        start a node

        :param name: the unique node name
        :return:  The dict representing the node
        """
        raise NotImplementedError

    def stop(self, name=None):
        """
        stops the node with the given name

        :param name:
        :return: The dict representing the node including updated status
        """
        raise NotImplementedError

    def info(self, name=None):
        """
        gets the information of a node with a given name

        :param name:
        :return: The dict representing the node including updated status
        """
        raise NotImplementedError

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
        raise NotImplementedError

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
        Gets the image with a given nmae
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
        wais till the given VM can be logged into

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
