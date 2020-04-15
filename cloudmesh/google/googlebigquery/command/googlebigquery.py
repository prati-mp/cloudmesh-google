from __future__ import print_function
from cloudmesh.shell.command import command
from cloudmesh.shell.command import PluginCommand
from cloudmesh.google.googlebigquery.Provider import Provider
from docopt import docopt
from cloudmesh.google.googlebigquery.api.manager import Manager
from cloudmesh.common.console import Console
from cloudmesh.common.util import path_expand
from pprint import pprint
from cloudmesh.common.debug import VERBOSE
from google.cloud import bigquery
from google.oauth2 import service_account

class GooglebigqueryCommand(PluginCommand):

    def get_options(self):
        args = docopt(__doc__)
        print(args)


    # noinspection PyUnusedLocal
    @command
    def do_googlebigquery(self, args, arguments):
        """
        ::

        Usage:
        googlebigquery create [DATASET_ID]
        googlebigquery list
        googlebigquery delete [DATASET_ID]
        googlebigquery listtables [DATASET_ID]
        googlebigquery describetable [DATASET_ID] [TABLE_ID]
        googlebigquery loadtable SOURCE_ID DATASET_ID TABLE_ID
        googlebigquery exporttable SOURCE PROJECT_ID DATASET_ID [TABLE_ID]
        googlebigquery runquery PROJECT_ID DATASET_ID [TABLE_ID] [QUERY_TXT]
        googlebigquery listjob [PROJECT_ID]


        Arguments:
            DATASET_ID              The Google bigquery dataset id.
            PROJECT_ID              The google big query project id
            TABLE_ID              The name of the table
            JOB_ID                  The job id in bigquery
            SOURCE_ID                  Local file which need to be load into bigquery table

        Description:
            googlebigquery create [DATASET_ID]
                Create a dataset in given project

            googlebigquery list
                List all datasets present in given project_id

            googlebigquery delete [DATASET_ID]
                Delete dataset from given project

            googlebigquery listtables DATASET_ID
                List all tables from given dataset and project

            googlebigquery loadtable SOURCE_ID DATASET_ID TABLE_ID
                Lod source file into given table

            googlebigquery runquery PROJECT_ID DATASET_ID [TABLE_ID] [QUERY_TXT]
                run given QUERY_TXT

            googlebigquery listjob [PROJECT_ID]
            List all jobs present in given project_id
        """

        #map_parameters(arguments, 'create', 'list', 'listtables', 'loadtable', 'exporttable', 'runquery','listjobs')

        googlebigquery = Provider()
        print(arguments)

        if arguments.loadtable:
            #googlebigquery loadtable SOURCE PROJECT_ID DATASET_ID [TABLE_ID]
            source_id = arguments.get('SOURCE_ID')
            #project_id = arguments.get('PROJECT_ID')
            dataset_id = arguments.get('DATASET_ID')
            table_id = arguments.get('TABLE_ID')
            print(source_id)
            result = googlebigquery.loaddata(source_id, dataset_id, table_id)

            if source_id is None or dataset_id is None or table_id is None:
                print("Please provide all parameters (SOURCE DATASET_ID [TABLE_ID])")
                try:
                    result = googlebigquery.loaddata(source_id, dataset_id, table_id)
                    print(result)
                finally:
                    return "Unhandled error"
        elif arguments.exporttable:
            # googlebigquery exporttable SOURCE PROJECT_ID DATASET_ID [TABLE_ID]
            source_id = arguments.get('SOURCE')
            project_id = arguments.get('PROJECT_ID')
            dataset_id = arguments.get('DATASET_ID')
            table_id = arguments.get('TABLE_ID')
            result = googlebigquery.loaddata(source_id, dataset_id, table_id)
            if source_id is None or project_id is None or dataset_id is None or table_id is None:
                print("Please provide all parameters (SOURCE DATASET_ID [TABLE_ID])")
                try:
                    result = googlebigquery.loaddata(source_id, dataset_id, table_id)
                    print(result)
                finally:
                    return "Unhandled error"
        elif arguments.list:
            # googlebigquery list project_id
            #source_id = arguments.get('SOURCE')
            project_id = arguments.get('PROJECT_ID')
            #dataset_id = arguments.get('DATASET_ID')
            #table_id = arguments.get('TABLE_ID')
            #result = googlebigquery.listdatasets()
            #print(result)
            try:
                result = googlebigquery.listdatasets()
                print(result)
            finally:
                return "Unhandled error"
        elif arguments.listtables:
            # googlebigquery list project_id
            #source_id = arguments.get('SOURCE')
            #project_id = arguments.get('PROJECT_ID')
            dataset_id = arguments.get('DATASET_ID')
            #table_id = arguments.get('TABLE_ID')
            #result = googlebigquery.listtables(dataset_id)
            #print(result)
            try:
                result = googlebigquery.listtables(dataset_id)
                print(result)
            finally:
                return "Unhandled error"
        elif arguments.describetable:
            # googlebigquery list project_id
            #source_id = arguments.get('SOURCE')
            #project_id = arguments.get('PROJECT_ID')
            dataset_id = arguments.get('DATASET_ID')
            table_id = arguments.get('TABLE_ID')
            #result = googlebigquery.describetable(dataset_id, table_id)
            #print(result)
            try:
                result = googlebigquery.describetable(dataset_id, table_id)
                print(result)
            finally:
                return "Unhandled error"
        else:
            print(self.get_options())

if __name__ == "__main__":
    print("In googlebigquery.py")
