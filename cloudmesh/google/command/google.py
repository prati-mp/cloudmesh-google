<<<<<<< HEAD
from __future__ import print_function
from cloudmesh.shell.command import command
from cloudmesh.shell.command import PluginCommand
from cloudmesh.common.console import Console
from cloudmesh.common.util import path_expand
=======
>>>>>>> 55c103f8f6d474020987013fb95c52785bd7db97
from pprint import pprint
from cloudmesh.common.util import banner
from cloudmesh.common.util import path_expand
from cloudmesh.configuration.Config import Config
from cloudmesh.google.storage.Provider import Provider
from cloudmesh.shell.command import PluginCommand
from cloudmesh.shell.command import command
from copy import deepcopy
from cloudmesh.common.variables import Variables
from cloudmesh.common.parameter import Parameter


class GoogleCommand(PluginCommand):
    """
    STUDENT - goes to google
    student download json google.json
    student does

        cms google yaml add google.json [--name=NAME]

            cloudmesh.storage.NAME

    content gets written into yaml file
    would you like to delete the file google.json (y)

    student say

    cms transfer xys

    system checks if ~/.google.json exists, if not, creates its

    now this json file is used for authentication ....async
    """

    # noinspection PyUnusedLocal
    @command
    def do_google(self, args, arguments):
        """
        ::

          Usage:
                google yaml add [FILE_JSON] [--storage=SERVICE]
                google yaml write [FILE_JSON] [--storage=SERVICE]
                google yaml list storage
                google list bucket
                google create bucket [--name=NAME] [--storage=SERVICE]


          This command does some useful things.

          Arguments:
              FILE   a file name

          Options:
              -f      specify the file



        """

        # variables = Variables()
        # arguments.output = Parameter.find("storage",
        #                                   arguments,
        #                                   variables,
        #                                   "google")

        name = arguments["--storage"] or "google"



        if arguments.yaml and arguments.add:
            banner("Read the  specification from json and write to yaml file")
            path = path_expand(arguments["FILE_JSON"] or "~/.cloudmesh/google.json")

            name = arguments["--storage"] or "google"
            Provider.json_to_yaml(name, filename=path)

        elif arguments.yaml and arguments.write:
            path = path_expand(arguments["FILE_JSON"] or "~/.cloudmesh/google.json")
            name = arguments["--storage"] or "google"

            banner(f"Write the  credential  from {name}  to the json file {path}")

            #    google yaml write FILE_JSON [--name=NAME]
            Provider.yaml_to_json(name, filename=path)

        elif arguments.yaml and arguments["list"] and arguments.storage:
            print("List all google storage providers")

            config = Config()

            storage = config["cloudmesh.storage"]
            for element in storage:
                if storage[element]["cm"]["kind"] == "google":
                    d = config[f"cloudmesh.storage.{element}"]
                    banner("cloudmesh.storage." + element)
                    e = deepcopy(d)
                    e["credentials"]["private_key"] = "*****"
                    print(Config.cat_dict(e))

        # elif arguments.yaml and arguments["list"]:
        #     print("Content of current yaml file")
        #     name = arguments["--storage"] or "google"
        #     config = Config()
        #
        #     credentials = config[f"cloudmesh.storage.{name}.credentials"]
        #     pprint(credentials)

        elif arguments["list"] and arguments.bucket:
            banner("Google storage Bucket List")
            provider = Provider(service=name)
            provider.list_bucket()

        elif arguments.create and arguments.bucket:
            bucket = arguments["--name"]
            name = arguments["--storage"] or "google"
            banner("Google storage create Bucket ")
            provider = Provider(service=name)
            provider.create_bucket(bucket)

        else:
            raise NotImplementedError

        return ""
