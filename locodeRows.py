# usage :
# python3 locodeRows.py  arg1 arg2
#   eg.  python3 locodeRows.py Bangladesh Malaysia
#
from pprint import pprint    # for pretty formatting
import requests              # for making REST API requests
import json                  # for converting json payloads to strings
import uuid                  # to create UUIDs for Astra connections
import os                    # for accessing creds
import warnings
import sys
warnings.filterwarnings('ignore') # to keep this notebook clean
def authenticate(path="/api/rest/v1/auth"):
    """
        This convenience function uses the v1 auth REST API to get an access token
        returns: an auth token; 30 minute expiration
    """
    url = baseURL + path # we still have to auth with the v1 API
    payload = {"username": userId,
               "password": passWord}
    headers = {'accept': '*/*',
               'content-type': 'application/json',
               'x-cassandra-request-id': UUID}
    # make auth request to Astra
    r = requests.post(url,
                      data=json.dumps(payload),
                      headers=headers)
    # raise any authentication errror

    if r.status_code != 201:
        raise Exception(r.text)
    # extract and return the auth token
    data = json.loads(r.text)
    return data["authToken"]

class Client:
    """
    An API Client for connecting to Stargate
    """
    def __init__(self, base_url, access_token, headers):
        self.base_url = base_url
        self.access_token = access_token
        self.headers = headers

    def post(self, payload={}, path=""):
        """
            Via the requests library, performs a post with the payload to the path
        """
        return requests.post(self.base_url + path,
                             data=json.dumps(payload),
                             headers=self.headers)

    def put(self, payload={}, path=""):
        """
            Via the requests library, performs a put with the payload to the path
        """
        return requests.put(self.base_url + path,
                            data=json.dumps(payload),
                            headers=self.headers)

    def patch(self, payload={}, path=""):
        """
            Via the requests library, performs a patch with the payload to the path
        """
        return requests.patch(self.base_url + path,
                              data=json.dumps(payload),
                              headers=self.headers)

    def get(self, payload={}, path=""):
        """
            Via the requests library, performs a get with the payload to the path
        """

        return requests.get(self.base_url + path,
                            data=json.dumps(payload),
                            headers=self.headers)

    def delete(self, payload={}, path=""):
        """
            Via the requests library, performs a delete with the payload to the path
        """
        return requests.delete(self.base_url + path,
                             data=json.dumps(payload),
                             headers=self.headers)
#
#Astra global
#
# https://c9f85285-5aa9-445b-b2c8-b45cd61705e3-us-east1.apps.astra.datastax.com/api/rest

with open('demo.env') as f:
  d = dict(x.rstrip().split(':', 1) for x in f)
  secure_connect_bundle = d.get('secure_connect_bundle')
  userId = d.get('userId')
  passWord = d.get('passWord')
  keySpaceId = d.get('keySpace')
  dbId = d.get('dbId')
  regionId = d.get('regionId')
  baseURL = f"https://{dbId}-{regionId}.apps.astra.datastax.com"

UUID = str(uuid.uuid1())

#get authorization Token
TOKEN = authenticate();

HEADERS = {'content-type': 'application/json',
           'x-cassandra-token': TOKEN}
stargate_client = Client(baseURL, TOKEN, HEADERS)

tableId = "covid19_by_geo_by_day"
columnId = "country_region"

#iterate through the passed parameter
position = 1
arguments = len(sys.argv) - 1

while (arguments >= position):
    primaryKey = sys.argv[position]
    #get rows:
    ROOT_PATH = f"/api/rest/v2/keyspaces/{keySpaceId}/{tableId}/{primaryKey}"
    response = stargate_client.get({}, ROOT_PATH)
    pprint(response.json())
    position = position +1
