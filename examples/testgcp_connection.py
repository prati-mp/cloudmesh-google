from cloudmesh.common.util import path_expand, banner


def gcstest():
    from google.cloud import storage



    # gcp = storage.Client.from_service_account_json('/Users/shreyansjain_2/cm/gcp/gcp_shrejain.json')
    """    
    
    defelop functoon 

    cms gcp credental read FILE.json [--config=CLUDMESHYAML]
        reads credentials form json file as provided by google and puts it in yaml file

    cms gcp credental write FILE.json [--config=CLUDMESHYAML]
        writes credentials from cloudmesh yaml file to json file

    """

    download_path = path_expand("~/.cloudmesh/download_file")
    json_path = path_expand("~/.cloudmesh/gcp.json")

    cheat = True
    proper = not cheat

    if cheat: # ;-)
        # credentials
        bucket_name = "cloudmesh_gcp"
        gcp = storage.Client.from_service_account_json(json_path)
    elif proper:
        config = Config()
        google = config(f'cloudmesh.storage.{name}')
        bucket_name = google["default"]["directory"]
        credentials = google["credentials"]

    # def list_buckets():
    #     """Lists all buckets."""
    #     storage_client = storage.Client()
    #     buckets = storage_client.list_buckets()
    #
    #     for bucket in buckets:
    #         print(bucket.name)
    # The name for the new bucket
    # bucket_name = 'my-new-bucket'

    # Creates the new bucket
    # try:
    #     bucket = storage_client.create_bucket(bucket_name)
    # except Exception as e:
    #     print(e)


    #print('Bucket {} created.'.format(bucket.name))
    # gsutil ls

    banner ("LITBUCKET")
    # printing buckets
    buckets = list(gcp.list_buckets())
    from pprint import pprint
    pprint(buckets)
    pprint (buckets.__dir__)
    pprint(dir(buckets))

    # print (help(buckets))
    banner ("OPEN")

    def open_bucket(name):
        try:
            bucket = gcp.get_bucket(name)
        except:
            return None

    banner ("does not exists")
    b = open_bucket("doesnotexist")
    print (b)

    banner("GET BUCKET")
    b = open_bucket(bucket_name)
    print(b)

    banner ("STOPR HERE")

    # Get the bucket that the file will be uploaded to.
    bucket = gcp.get_bucket(bucket_name)

    # list files
    blobs = list(bucket.list_blobs())
    print(blobs)

    message = 'test21'
    blob = bucket.blob(message)
    blob.upload_from_string(message)

    # bucket.delete_blob('test12')
    # print('Blob {} deleted.'.format(blob_name))


   # bucket.delete_blob('test123')

    #download file
    blob2 = bucket.get_blob('test1235')

    blob2.download_to_filename(download_path)
    #blob2.download_to_file()

    # Create a new folder.
    folder = 'a16/a17/'
    blob1 = bucket.blob(folder)
    blob1.upload_from_string('')

# Create a new blob and upload the file's content.




    #blob.delete('test12')
    #print('Blob {} deleted.'.format(blob_name))

gcstest()
