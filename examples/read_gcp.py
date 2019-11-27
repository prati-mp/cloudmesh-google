from cloudmesh.configuration.Config import Config
config = Config()
gcp = config["cloudmesh.storage.gcp.cm"]
print(gcp)

gcpCredentials = config["cloudmesh.storage.gcp.credentials"]
print(gcpCredentials)

##
"""
    flow = flow_from_clientsecrets(filename, scope, message=message,          #Change this to use jason as oblect
                                   cache=cache, redirect_uri=redirect_uri,
                                   device_uri=device_uri)
    credentials = flow.step2_exchange(code, http=http)
    return credentials




import json
with open('result.json', 'w') as fp:
    json.dump(box, fp)

#print(json.dumps(box))
from io import StringIO
io = StringIO()
json.dump(box, io)
print(io.getvalue())

"""