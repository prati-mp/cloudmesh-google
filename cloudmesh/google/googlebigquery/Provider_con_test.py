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

config="~/.cloudmesh/cloudmesh.yaml"
config2 = Config(config_path=config)
print(config2['cloudmesh.cloud.google.credentials.path_to_json_file'])
print(config2['cloudmesh.cloud.google.credentials.project'])
service_account_file = config2['cloudmesh.cloud.google.credentials.path_to_json_file']
print(service_account_file)
credentials = service_account.Credentials.from_service_account_file(service_account_file)
project_id = config2['cloudmesh.cloud.google.credentials.project']
client = bigquery.Client(credentials=credentials,project=project_id)

#query_txt = query_id
query_txt="SELECT CONCAT( 'https://stackoverflow.com/questions/', CAST(id as STRING)) as url,  view_count FROM `bigquery-public-data.stackoverflow.posts_questions` WHERE tags like '%google-bigquery%' ORDER BY view_count DESC LIMIT 10"
        #client = bigquery.Client(credentials=credentials, project=project_id)
query_job = client.query(query_txt)
results = query_job.result()
for row in results:
    print("{} : {} views".format(row.url, row.view_count))
datasets = list(client.list_datasets())
results = datasets
project = client.project
if datasets:
    print("Datasets in project {}:".format(project))
    for dataset in datasets:
        print("\t{}".format(dataset.dataset_id))
else:
     print("{} project does not contain any datasets.".format(project))

print(project_id)
table_id1 ='employee_table'
dataset_id = 'emp_table'
table_id = client.project + "." + dataset_id + "." + table_id1
table = client.get_table(table_id)
# Table description
print("Got table", table.table_id)
print("Table schema: ", table.schema)
print("Table description:", table.description)
print("Table number of rows", table.num_rows)

'''
credentials = service_account.Credentials.from_service_account_file('C:\Deepak\MS\BQ\cmddeopura-019c0c55e161.json')
project_id = 'cmddeopura'
client = bigquery.Client(credentials= credentials,project=project_id)
'''

if __name__ == "__main__":
    print("In Provider")
