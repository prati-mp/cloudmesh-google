from copy import deepcopy

from cloudmesh.common.util import banner
from cloudmesh.common.util import path_expand
from cloudmesh.configuration.Config import Config
from cloudmesh.google.storage.Provider import Provider
from cloudmesh.shell.command import PluginCommand
from cloudmesh.shell.command import command
from cloudmesh.shell.command import map_parameters


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
                google config add [FILE_JSON] [--storage=SERVICE]
                google config write [FILE_JSON] [--storage=SERVICE]
                google config list storage
                google config list credentials
                google list
                google create [--name=NAME] [--storage=SERVICE]
                google bigquery delete


          This command does some useful things.

          Arguments:
              FILE   a file name

          Options:
              -f      specify the file

          Description:
          
            google config add [FILE_JSON] [--storage=SERVICE]
            
                TODO 
                
            google config write [FILE_JSON] [--storage=SERVICE]
            
                TODO 
                
            google config list storage
            
                TODO 
                
            google config list credentials
            
                TODO 
                
            google list
            
                TODO 
                
            google create [--name=NAME] [--storage=SERVICE]
            
                TODO 
                
        """

        # variables = Variables()
        # arguments.output = Parameter.find("storage",
        #                                   arguments,
        #                                   variables,
        #                                   "google")



        map_parameters(arguments,
                       'storage',
                       'name')


        name = arguments.storage or "google"


        if arguments.bigquery:

            from cloudmesh.google.bigquerey.interpreter import Interpreter
            result = Interpreter.interprete(arguments)

            return result


        elif arguments.config and arguments.add:
            banner("Read the  specification from json and write to yaml file")
            path = path_expand(arguments.FILE_JSON or "~/.cloudmesh/google.json")

            name = arguments.storage or "google"
            Provider.json_to_yaml(name, filename=path)

        elif arguments.config and arguments.write:
            path = path_expand(arguments.FILE_JSON or "~/.cloudmesh/google.json")
            name = arguments.storage or "google"

            banner(f"Write the  credential  from {name}  to the json file {path}")

            #    google yaml write FILE_JSON [--name=NAME]
            Provider.yaml_to_json(name, filename=path)

        elif arguments.config and arguments["list"] and arguments.storage:
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

        elif arguments.config and arguments["list"] and arguments.credentials:
             print("Content of current yaml file")
             name = arguments.storage or "google"
             config = Config()

             credentials = config[f"cloudmesh.storage.{name}.credentials"]

             print(Config.cat_dict(credentials))

        elif arguments["list"]:
            banner("Google storage Bucket List")
            provider = Provider(service=name)
            provider.list_bucket()

        elif arguments.create:
            bucket = arguments.name
            name = arguments.storage or "google"
            banner("Google storage create Bucket ")
            provider = Provider(service=name)
            provider.create_bucket(bucket)

        else:
            raise NotImplementedError

        return ""
