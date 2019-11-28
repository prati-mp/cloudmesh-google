from __future__ import print_function
from cloudmesh.shell.command import command
from cloudmesh.shell.command import PluginCommand
from cloudmesh.google.api.manager import Manager
from cloudmesh.common.console import Console
from cloudmesh.common.util import path_expand
from pprint import pprint
from cloudmesh.common.debug import VERBOSE
from cloudmesh.configuration.Config import Config


class GoogleCommand(PluginCommand):

    # noinspection PyUnusedLocal
    @command
    def do_google(self, args, arguments):
        """
        ::

          Usage:
                google yaml write FILE_JSON [--name=NAME]
                google yaml list [--name=NAME]
                google yaml read FILE_JSON [--name=NAME]
                google list storage

          This command does some useful things.

          Arguments:
              FILE   a file name

          Options:
              -f      specify the file



        """

        VERBOSE(arguments)

        name = arguments["--name"] or "google"

        if arguments.yaml and arguments.write:
            print("Read the  specification from yaml and write to json file")
            raise NotImplementedError

        elif arguments.yaml and arguments.read:
            print("Read the  specification from json and write to yaml file")
            raise NotImplementedError

        elif arguments.list and arguments.storage:
            print("List all google storage providers")

            config = Config()

            storage = config["cloudmesh.storage"]
            for element in storage:
                if storage[element]["cm"]["kind"] == "google":
                    #pprint(storage[element])
                    import oyaml as yaml
                    kluge = yaml.dump(config[f"cloudmesh.storage.{element}"], default_flow_style=False, indent=2)
                    print (kluge)
                    print (Config.cat_lines(kluge, mask_secrets=True))



        elif arguments.list:
            print("Content of current yaml file")

            config = Config()

            credentials = config[f"cloudmesh.storage.{name}.credentials"]
            pprint(credentials)


        else:
            raise NotImplementedError

        return ""
