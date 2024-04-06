import requests
import json
import os
from dotenv import load_dotenv

from logger import logger

# from utils.microservice.logger.Log import Log

# Constants values to requests
load_dotenv()
data_bus_url = os.getenv("DATA_BUS_URL")
data_bus_port = os.getenv("DATA_BUS_PORT")
DATA_BUS_URL = "http://" + data_bus_url + ":" + data_bus_port \
    if data_bus_port is not None else "http://" + data_bus_url
JSONheader = {'content-type': 'application/json'}
textHeader = {'content-type': 'text/plain'}
textJSONHeader = {'content-type': 'text/json'}
data_bus_authentication = os.getenv("data_bus_authentication")
USER_NAME = os.getenv("USER_NAME")
USER_PASSWORD = os.getenv("USER_PASSWORD")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")


def getAuthenticationToken():
    # Define username to request token
    userName = USER_NAME
    # Define password to request token
    userPassword = str(USER_PASSWORD)
    # Create message to make request
    authRequest = {
        "User": {
            "UserName": userName,
            "UserPassword": userPassword
        },
        "GrantType": "password"
    }
    # Transform dict into JSON
    authRequestJSON = json.dumps(authRequest)
    # Get url to take acess token
    url = data_bus_authentication
    # Get authTokenJSON
    authTokenJSON = postRequest(url, authRequestJSON)
    # Transform token info to dict
    authTokenDict = json.loads(authTokenJSON)
    accessToken = authTokenDict['accessToken']
    os.environ["ACCESS_TOKEN"] = accessToken
    return accessToken


def postRequest(url, json):
    # Assemble request path
    url = DATA_BUS_URL + url
    # Send a request post
    response = requests.post(url, data=json, headers=JSONheader)
    # # Set log indicating operation
    # Log().info("POST request made to route " + url)
    # If status code is not 2XX, there is return data
    if response.ok:
        # Get post result as JSON
        return response.text
    else:
        # If incorrect, return None
        return None


def postRequestAuth(url, json):
    # Create auth header
    authHeader = {'Authorization': 'Bearer ' + ACCESS_TOKEN,
                  'Content-Type': 'application/json'}
    # Assemble request path
    url = DATA_BUS_URL + url
    # Send a request post
    response = requests.post(url, data=json, headers=authHeader)
    # # Set log indicating operation
    # Log().info("POST request made to route " + url)
    # If status code is not 2XX, there is return data
    if response.ok:
        # Get post result as JSON
        return response.text
    else:
        # If incorrect, try to get another auth token
        new_access_token = getAuthenticationToken()
        logger.info(f"New ACCESS TOKEN has been obtained: {new_access_token}")
        authHeader = {'Authorization': 'Bearer ' + new_access_token,
                      'Content-Type': 'application/json'}
        response = requests.post(url, data=json, headers=authHeader)
        if response.ok:
            # Log().info("POST request successful made to route " + url)
            return response.text
        else:
            return None


def getRequest(url, parameters=None):
    # Assemble request path
    url = DATA_BUS_URL + url
    # Send a request post
    response = requests.get(url, params=parameters)
    # # Set log indicating operation
    # Log().info(f"GET request made to route {url} with parameters {str([(par,val) for par, val in parameters.items()])}.")
    # If status code is not 2XX, there is return data
    if response.ok:
        # Get post result as JSON
        return response.text
    else:
        # If incorrect, return None
        return None
