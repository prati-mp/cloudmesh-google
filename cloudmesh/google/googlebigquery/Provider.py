from cloudmesh.configuration.Config import Config
import uuid
from botocore.exceptions import ClientError
from cloudmesh.common.debug import VERBOSE
from cloudmesh.mongo.DataBaseDecorator import DatabaseUpdate
#import psycopg2
import re
import base64
from google.cloud import bigquery
from google.oauth2 import service_account

class Provider(object):

    def __init__(self, service='compute'):
        VERBOSE("initialize google big query manager")
        self.service_account_file = None
        self.project_id = None
        #self.region = None
        self.config = Config()

        print(self.service_account_file)
        self.service_account_file = self.config['cloudmesh.cloud.google.credentials.path_to_json_file']
        self.credentials = service_account.Credentials.from_service_account_file(self.service_account_file)
        self.project_id = self.config['cloudmesh.cloud.google.credentials.project']
        self.client = bigquery.Client(credentials=self.credentials, project=self.project_id )

        print(self.project_id)


    def update_dict(self, d):
        d["cm"] = {
            "kind": "bigquery",
            "name": "bigquery",
            "cloud": "google",
        }
        return d


    def update_status(self, results=None, name=None, status=None):
        return self.update_dict(
            {"cloud": "google",
             "kind": "bigquery",
             "name": name,
             "status": status,
             "results": results,
             })

    def listdatasets(self):
        print("Listing dataset")
        VERBOSE("in list dataset")
        results = {}
        #client = bigquery.Client(credentials=credentials, project=project_id)
        datasets = list(self.client.list_datasets())
        results = datasets
        project = self.client.project
        if datasets:
            print("Datasets in project {}:".format(project))
            for dataset in datasets:
                print("\t{}".format(dataset.dataset_id))
        else:
            print("{} project does not contain any datasets.".format(project))

    def loaddata(self, source_id, dataset_id, table_id):
        print("Loading data")
        VERBOSE("in load data from locale")
        results = {}
        # client = bigquery.Client()
        filename = str(source_id)
        print(filename)
        # dataset_id = 'emp_table'
        # table_id = 'employee_table'
        # client = bigquery.Client(credentials=credentials, project=project_id)
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        job_config.autodetect = True
        with open(filename, "rb") as source_file:
            job = self.client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()  # Waits for table load to complete.
        print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))

    def runsamplequery(self,dataset_id, query_id):
        query_txt = query_id
        # query_txt="SELECT CONCAT( 'https://stackoverflow.com/questions/', CAST(id as STRING)) as url,  view_count FROM `bigquery-public-data.stackoverflow.posts_questions` WHERE tags like '%google-bigquery%' ORDER BY view_count DESC LIMIT 10"
        #client = bigquery.Client(credentials=credentials, project=project_id)
        query_job = self.client.query(query_txt)
        results = query_job.result()
        for row in results:
            print("{} : {} views".format(row.url, row.view_count))

    def listtables(self, dataset_id):
        tables = self.client.list_tables(dataset_id)
        print("Tables contained in dataset", dataset_id)
        for table in tables:
            print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    def describetable(self, dataset_id, table_id):
        table_id = self.client.project + "." + dataset_id + "." + table_id
        print(table_id)
        #table = self.client.get_table(table_id)

        # Table description
        table = self.client.get_table(table_id)
        print("Got table", table.table_id)
        print("Table schema: ", table.schema)
        print("Table description:", table.description)
        print("Table number of rows", table.num_rows)


    def exportdata(self, source_id, project_id, dataset_id, table_id):
        table_id = self.client.project + "." + dataset_id + "." + table_id
        table = self.client.get_table(table_id)
        # Table description
        print("Got table", table.table_id)
        print("Table schema: ", table.schema)
        print("Table description:", table.description)
        print("Table number of rows", table.num_rows)

if __name__ == "__main__":
    print("In Provider")
