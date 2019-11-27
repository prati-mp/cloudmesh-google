from cloudmesh.common.util import path_expand

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

    cheat = True
    proper = not cheat

    if cheat: # ;-)
        # credentials
        json_path=path_expand("~/.cloudmesh/gcp.json")
        gcp = storage.Client.from_service_account_json(json_path)
    elif proper:
        raise NotImplementedError
        # read from yaml file
        # ;-)

    # printing buckets
    buckets = list(gcp.list_buckets())
    print(buckets)

    # Get the bucket that the file will be uploaded to.
    bucket = gcp.get_bucket('cloudmesh_gcp')

    # list files
    blobs = list(bucket.list_blobs())
    print(blobs)

    message = 'test12'
    blob = bucket.blob(message)
    blob.upload_from_string(message)

    # bucket.delete_blob('test12')
    # print('Blob {} deleted.'.format(blob_name))


   # bucket.delete_blob('test123')

    #download file
    blob2 = bucket.get_blob('test12')
    blob2.download_to_filename('/Users/shreyansjain_2/cm/gcp/downloadfile')
    #blob2.download_to_file()

    # Create a new folder.
    folder = 'a10/a9/'
    blob1 = bucket.blob(folder)
    blob1.upload_from_string('')

# Create a new blob and upload the file's content.




    #blob.delete('test12')
    #print('Blob {} deleted.'.format(blob_name))


gcstest();
