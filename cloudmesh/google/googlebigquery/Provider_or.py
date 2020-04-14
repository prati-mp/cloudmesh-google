from cloudmesh.management.configuration.config import Config
import uuid
import boto3
from botocore.exceptions import ClientError
from cloudmesh.common.debug import VERBOSE
from cloudmesh.mongo.DataBaseDecorator import DatabaseUpdate
import psycopg2
import re
import base64
from google.cloud import bigquery
from google.oauth2 import service_account

class Provider(object):

    def __init__(self, service='bigquery', config="~/.cloudmesh/cloudmesh.yaml"):
        VERBOSE("initialize google big query manager")
        self.service_account_file = None
        self.access_key = None
        self.region = None
        self.config = Config(config_path=config)

        print(self.service_account_file)
        self.service_account_file = self.config[
            'cloudmesh.cloud.bigquery.credentials.service_account_file_path']
        self.credentials = service_account.Credentials.from_service_account_file(self.service_account_file)
        self.client = bigquery.Client(
            credentials=self.credentials
        )
        # print(self.key_id)

        self.ec2_client = bigquery.Client(
            'ec2',
            region_name=self.region,
            aws_access_key_id=self.key_id,
            aws_secret_access_key=self.access_key
        )

        self.ec2_resource = bigquery.Client(
            'ec2',
            region_name=self.region,
            aws_access_key_id=self.key_id,
            aws_secret_access_key=self.access_key
        )

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


    @DatabaseUpdate()
    def list_datasets(self):
        VERBOSE("in list dataset")
        results = {}
        try:
            results['dataset'] = "Error executing command"
            results = self.client.list_datasets()
            print(results['dataset'])
            return self.update_status(results=results,
                                      name='all',
                                      status="listingdataset")
        except ClientError as e:
            if e.response['Error']['Code'] == 'DatasetNotFound':
                 results['dataset'] = "No dataset exist"
            return self.update_status(results=results,
                                      name="all",
                                      status="listingdataset")

    @DatabaseUpdate()
    def describe_cluster(self, cluster_id):
        VERBOSE("in describe specific cluster")
        results = {}
        try:
            results['Clusters'] = "Error executing command"
            results = self.client.describe_clusters(
                ClusterIdentifier=cluster_id)
            print(results['Clusters'])
            # return [{"cm": {"cloud": "aws", "kind": "redshift", "name": cluster_id},
            #         'results': results['Clusters']}]
            return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Describing")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ClusterNotFound':
                results['Clusters'] = "Cluster Not Found"
            return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Describing")

    @DatabaseUpdate()
    def loaddata(source_id, project_id, dataset_id, table_id):
        print("Loading data")
        # client = bigquery.Client()
        filename = source_id
        # dataset_id = 'emp_table'
        # table_id = 'employee_table'
        client = bigquery.Client(credentials=credentials, project=project_id)
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        job_config.autodetect = True
        with open(filename, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()  # Waits for table load to complete.
        print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))

    @DatabaseUpdate()
    def create_multi_node_cluster(self, db_name, cluster_id, cluster_type, node_type, user_name, passwd, node_count):
        VERBOSE("in create multi node cluster")
        if cluster_type is None:
            cluster_type = 'multi-node'

        results = {}
        try:
            results['Clusters'] = "Error creating cluster"

            results = self.client.create_cluster(
                DBName=db_name,
                ClusterIdentifier=cluster_id,
                ClusterType=cluster_type,
                NodeType=node_type,
                MasterUsername=user_name,
                MasterUserPassword=passwd,
                Port=5439,
                AllowVersionUpgrade=True,
                NumberOfNodes=node_count,
                PubliclyAccessible=True,
                Encrypted=False
            )
            return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Creating")

        except ClientError as e:
            results['Clusters'] = results['Clusters'] + ' : ' + e.response
            return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Error Creating")

    @DatabaseUpdate()
    def delete_cluster(self, cluster_id):
        VERBOSE("in delete cluster")
        results = {}
        try:
            results['Clusters'] = "Error deleting cluster"
            results = self.client.delete_cluster(
                ClusterIdentifier=cluster_id,
                SkipFinalClusterSnapshot=False,
                FinalClusterSnapshotIdentifier=cluster_id + str(uuid.uuid1()),
                FinalClusterSnapshotRetentionPeriod=2
            )
            return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Deleting")
        except ClientError as e:
            results['Clusters'] = results['Clusters'] + ' : ' + e.response
            return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Error Deleting")


    @DatabaseUpdate()
    def resize_cluster_to_multi_node(self, cluster_id, cluster_type, node_count, node_type):
        VERBOSE("in resize cluster to multi-node")
        results = {}
        try:
            results['Clusters'] = "Error resizing cluster to multi-node"

            results = self.client.modify_cluster(
                ClusterIdentifier=cluster_id,
                ClusterType=cluster_type,
                NumberOfNodes=node_count,
                NodeType=node_type
            )

            return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Changing node count")

        except ClientError as e:
            results['Clusters'] = results['Clusters'] + ' : ' + e.response
            return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Error Resizing")


    @DatabaseUpdate()
    def modify_cluster(self, cluster_id, new_pass):
        VERBOSE("in modify")

        results = {}
        try:
            results['Clusters'] = "Error modifying cluster password"
            results = self.client.modify_cluster(
                ClusterIdentifier=cluster_id,
                MasterUserPassword=new_pass
            )
            return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Modifying password")

        except ClientError as e:
            results['Clusters'] = results['Clusters'] + ' : ' + e.response
            return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Error changing password")
    @DatabaseUpdate()
    def rename_dataset(self, dataset_id, new_id):
        VERBOSE("in rename")

        results = {}
        try:
            results['dataset'] = "Error renaming dataset"
            results = self.client.modify_cluster(
                ClusterIdentifier=cluster_id,
                NewClusterIdentifier=new_id,
            )
            return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Renaming")

        except ClientError as e:
            results['Clusters'] = results['Clusters'] + ' : ' + e.response
            return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Error renaming cluster")

    @DatabaseUpdate()
    def allow_access(self, cluster_id):
        VERBOSE("in allow access")
        desc_response = self.client.describe_clusters(
            ClusterIdentifier=cluster_id
        )

        vpc_id = desc_response['Clusters'][0]['VpcId']

        vpc = self.ec2_resource.Vpc(vpc_id)

        security_group_iterator = vpc.security_groups.all()
        for s in security_group_iterator:
            print(s)

        security_group_iterator2 = vpc.security_groups.filter(
            GroupNames=[
                'default',
            ]
        )
        print(security_group_iterator2)
        for s2 in security_group_iterator2:
            print(s2)

        grp_id_list = re.findall(r"'(.*?)'", str(s2), re.DOTALL)
        grp_id = grp_id_list[0]

        security_group = self.ec2_resource.SecurityGroup(grp_id)

        try:
            sec_grp_ingress_response = security_group.authorize_ingress(
                GroupId=grp_id,
                IpPermissions=[
                    {
                        'IpProtocol': 'tcp',
                        'FromPort': 5439,
                        'ToPort': 5439,
                        'IpRanges': [
                            {
                                'CidrIp': '0.0.0.0/0',
                                'Description': 'all ext'
                            },
                        ],
                        'UserIdGroupPairs': [{'GroupId': grp_id, 'VpcId': vpc_id}]
                    }],
                )
        except ClientError as err:
            sec_grp_ingress_response = err.response

        results = sec_grp_ingress_response
        # print(sec_grp_ingress_response)

        return self.update_status(results=results,
                                  name=cluster_id,
                                  status="Allowing access")

    @DatabaseUpdate()
    def create_demo_schema(self, cluster_id, db_name, host, port, user_name, passwd):
        VERBOSE("in create schema demo")
        print("in create demo schema")
        results = {}
        try:
            results['Clusters'] = "Error creating demo schema"
            conn = psycopg2.connect(dbname=db_name, host=host, port=port,
                                    user=user_name, password=passwd)

            print("connected")
            cur = conn.cursor()
            sql_cr = "CREATE TABLE emp (empid INT, empname VARCHAR(80));"
            sql_ins1 = "INSERT INTO emp values (10, 'smith');"
            sql_ins2 = "INSERT INTO emp values (20, 'jones');"
            sql_ins3 = "INSERT INTO emp values (30, 'scott');"
            sql_ins4 = "INSERT INTO emp values (40, 'adams');"

            try:
                print("exec sql")
                cur.execute(sql_cr)
                cur.execute(sql_ins1)
                cur.execute(sql_ins2)
                cur.execute(sql_ins3)
                cur.execute(sql_ins4)
                results = "Successfully created demo schema"
                cur.close()
                conn.commit()
                conn.close()
            except Exception as err:
                print("err executing sql")
                print("Error executing sql", err)
                cur.close()
                conn.close()
                return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Unable to execute query")

            return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Query results")

        except Exception as err:
            print("unable to connect")
            print("Unable to connect", err)
            return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Unable to connect")
    @DatabaseUpdate()
    def delete_demo_schema(self,  cluster_id, db_name, host, port, user_name, passwd):
        VERBOSE("in delete demo")
        print("in delete demo schema")

        results = {}
        try:
            results['Clusters'] = "Error deleting demo schema"
            conn = psycopg2.connect(dbname=db_name, host=host, port=port,
                                    user=user_name, password=passwd)

            cur = conn.cursor()
            sql = "DROP TABLE emp;"
            try:
                cur.execute(sql)
                results = "Successfully deleted demo schema"
                cur.close()
                conn.commit()
                conn.close()
            except Exception as err:
                print("Unable to execute", err)
                cur.close()
                conn.close()
                return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Unable to execute query")

            return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Query results")

        except Exception as err:
            print("Unable to connect", err)
            return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Unable to connect")


    @DatabaseUpdate()
    def runselectquery_text(self, cluster_id, db_name, host, port, user_name, passwd, query_text):
        VERBOSE("in runselectquery")
        print("In runselectquerytext")

        results = {}
        try:
            results['Clusters'] = "Error running SELECT query"
            conn = psycopg2.connect(dbname=db_name, host=host, port=port,
                                    user=user_name, password=passwd)

            cur = conn.cursor()

            print(query_text)
            sql = query_text

            try:
                cur.execute(sql)
                results = cur.fetchall()
                print(results)
                cur.close()
                conn.close()
            except Exception as err:
                print("unable to execute query")
                print("Unable to execute", err)
                cur.close()
                conn.close()
                return self.update_status(results=results,
                                        name=cluster_id,
                                        status="Unable to execute query")

            return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Query results")

        except Exception as err:
            print("unable to connect")
            print("Unable to connect", err)
            return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Unable to connect")
    @DatabaseUpdate()
    def runddl(self, cluster_id, db_name, host, port, user_name, passwd, b64_sql_file_contents):
        VERBOSE("in runsql - DDL")
        print("In runsql - DDL")
        # print(args)
        results = {}
        try:
            results['Clusters'] = "Error running DDL statements from file"

            conn = psycopg2.connect(dbname=db_name, host=host, port=port,
                                    user=user_name, password=passwd)

            cur = conn.cursor()

            sql_file_contents = base64.b64decode(b64_sql_file_contents).decode('ascii')
            # in the future for DML, we may need to base64.b64decode(b64_sql_file_contents).decode("utf-8", "ignore")
            # all SQL statements (split on ';')
            sql_stmts = sql_file_contents.replace("\n", "").split(';')

            for stmt in sql_stmts:
                # This will skip and report errors
                # For example, if the tables do not yet exist, this will skip over
                # the DROP TABLE commands
                if stmt is None or len(stmt.strip()) == 0:
                    continue
                try:
                    cur.execute(stmt + ";")
                    results[stmt] = "success"
                    results['success'] = "ok"
                except psycopg2.InterfaceError as err:
                    print("error in interface:", err)
                    results['Clusters'] = results['Clusters'] + str(err)
                    conn.rollback()
                    continue
                except psycopg2.DatabaseError as err:
                    print("error related to DB:", err)
                    results['Clusters'] = results['Clusters'] + str(err)
                    conn.rollback()
                    continue
                except psycopg2.DataError as err:
                    print("error in data:", err)
                    results['Clusters'] = results['Clusters'] + str(err)
                    conn.rollback()
                    continue
                except psycopg2.OperationalError as err:
                    print("error related to db operation:", err)
                    results['Clusters'] = results['Clusters'] + str(err)
                    conn.rollback()
                    continue
                except psycopg2.IntegrityError as err:
                    print("error in data integrity:", err)
                    results['Clusters'] = results['Clusters'] + str(err)
                    conn.rollback()
                    continue
                except psycopg2.InternalError as err:
                    print("error: internal, usually type of DB Error:", err)
                    results['Clusters'] = results['Clusters'] + str(err)
                    conn.rollback()
                    continue
                except psycopg2.ProgrammingError as err:
                    print("error in Programming:", err)
                    results['Clusters'] = results['Clusters'] + str(err)
                    conn.rollback()
                    continue
                except psycopg2.NotSupportedError as err:
                    print("error - not supported feature/operation: ", err)
                    results['Clusters'] = results['Clusters'] + str(err)
                    conn.rollback()
                    continue

            cur.close()
            conn.commit()
            conn.close()
            results['Clusters'] = "DDL run successfully"
            return self.update_status(results=results,
                                  name=cluster_id,
                                  status="SQL Execution")


        except Exception as err:
            print("unable to connect")
            print("Unable to connect", err)
            return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Unable to connect")

    @DatabaseUpdate()
    def rundml(self, cluster_id, db_name, host, port, user_name, passwd, b64_sql_file_contents):
        VERBOSE("in runsql - DML")
        print("In runsql - DML")
        results = {}
        try:
            results['Clusters'] = "Error running DML statements from file"
            conn = psycopg2.connect(dbname=db_name, host=host, port=port,
                                    user=user_name, password=passwd)

            cur = conn.cursor()

            sql_file_contents = base64.b64decode(b64_sql_file_contents).decode('utf-8')
            # in the future for DML, we may need to base64.b64decode(b64_sql_file_contents).decode("utf-8", "ignore")
            # all SQL statements (split on ';')
            sql_stmts = sql_file_contents.replace("\n", "").split(';')

            for stmt in sql_stmts:
                # This will skip and report errors
                # For example, if the tables do not yet exist, this will skip over
                # the DROP TABLE commands
                if stmt is None or len(stmt.strip()) == 0:
                    continue
                try:
                    cur.execute(stmt)

                except psycopg2.InterfaceError as err:
                    print("error in interface:", err)
                    results['Clusters'] = results['Clusters'] + str(err)
                    conn.rollback()
                    continue
                except psycopg2.DatabaseError as err:
                    print("error related to DB:", err)
                    results['Clusters'] = results['Clusters'] + str(err)
                    conn.rollback()
                    continue
                except psycopg2.DataError as err:
                    print("error in data:", err)
                    results['Clusters'] = results['Clusters'] + str(err)
                    conn.rollback()
                    continue
                except psycopg2.OperationalError as err:
                    print("error related to db operation:", err)
                    results['Clusters'] = results['Clusters'] + str(err)
                    conn.rollback()
                    continue
                except psycopg2.IntegrityError as err:
                    print("error in data integrity:", err)
                    results['Clusters'] = results['Clusters'] + str(err)
                    conn.rollback()
                    continue
                except psycopg2.InternalError as err:
                    print("error: internal, usually type of DB Error:", err)
                    results['Clusters'] = results['Clusters'] + str(err)
                    conn.rollback()
                    continue
                except psycopg2.ProgrammingError as err:
                    print("error in Programming:", err)
                    results['Clusters'] = results['Clusters'] + str(err)
                    conn.rollback()
                    continue
                except psycopg2.NotSupportedError as err:
                    print("error - not supported feature/operation: ", err)
                    results['Clusters'] = results['Clusters'] + str(err)
                    conn.rollback()
                    continue

            cur.close()
            conn.commit()
            conn.close()
            results['Clusters'] = "DML run successfully"
            return self.update_status(results=results,
                                  name=cluster_id,
                                  status="SQL Execution")


        except Exception as err:
            print("unable to connect")
            print("Unable to connect", err)
            return self.update_status(results=results,
                                      name=cluster_id,
                                      status="Unable to connect")


if __name__ == "__main__":
    print("In Provider")
