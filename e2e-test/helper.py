
import json
import uuid
import random
import string
import wds_client
import requests
import time
import logging

logging.basicConfig(level=logging.DEBUG)

workspace_manager_url = ""
rawls_url = ""
leo_url = ""

def setup(bee_name):
# define major service endpoints based on bee name
    global workspace_manager_url
    workspace_manager_url = f"https://workspace.{bee_name}.bee.envs-terra.bio"
    logging.debug(f"workspace_manager_url: {workspace_manager_url}")
    global rawls_url
    rawls_url = f"https://rawls.{bee_name}.bee.envs-terra.bio"
    logging.debug(f"rawls_url: {rawls_url}")
    global leo_url
    leo_url = f"https://leonardo.{bee_name}.bee.envs-terra.bio"
    logging.debug(f"leo_url: {leo_url}")
    return workspace_manager_url, rawls_url, leo_url

# CREATE WORKSPACE ACTION
def create_workspace(cbas, billing_project_name, header):
    # create a new workspace, need to have attributes or api call doesnt work
    api_call2 = f"{rawls_url}/api/workspaces";
    request_body= {
      "namespace": billing_project_name, # Billing project name
      "name": f"api-workspace-{''.join(random.choices(string.ascii_lowercase, k=5))}", # workspace name
      "attributes": {}};

    response = requests.post(url=api_call2, json=request_body, headers=header)
    logging.debug(f"url: {api_call2}")
    logging.debug(f"headers: {header}")
    logging.debug(f"response: {response}")
    logging.debug(f"request_body: {request_body}")
    
    #example json that is returned by request: 'attributes': {}, 'authorizationDomain': [], 'bucketName': '', 'createdBy': 'yulialovesterra@gmail.com', 'createdDate': '2023-08-03T20:10:59.116Z', 'googleProject': '', 'isLocked': False, 'lastModified': '2023-08-03T20:10:59.116Z', 'name': 'api-workspace-1', 'namespace': 'yuliadub-test2', 'workspaceId': 'ac466322-2325-4f57-895d-fdd6c3f8c7ad', 'workspaceType': 'mc', 'workspaceVersion': 'v2'}
    json2 = response.json()
    logging.debug(f"json response: {json2}")
    data = json.loads(json.dumps(json2))
    
    logging.debug(f"data['workspaceId']:  {data['workspaceId']}")
    
    # enable CBAS if specified
    if cbas is True:
        logging.info(f"Enabling CBAS for workspace {data['workspaceId']}")
        api_call3 = f"{leo_url}/api/apps/v2/{data['workspaceId']}/terra-app-{str(uuid.uuid4())}";
        request_body2 = {
          "appType": "CROMWELL"
        } 
        
        response = requests.post(url=api_call3, json=request_body2, headers=header)
        # will return 202 or error
        logging.debug(response)

    return data['workspaceId'], data['name']

# GET WDS OR CROMWELL ENDPOINT URL FROM LEO
def get_app_url(workspaceId, app, azure_token):
    """"Get url for wds/cbas."""
    uri = f"{leo_url}/api/apps/v2/{workspaceId}?includeDeleted=false"
    headers = {"Authorization": "Bearer " + azure_token,
               "accept": "application/json"}

    app_type = "CROMWELL" if app != 'wds' else app.upper();
    logging.info(f"App type: {app_type}")

    #TODO: can this get into an infinite loop?
    while True:
        response = requests.get(uri, headers=headers)
        logging.debug(response)
        status_code = response.status_code

        if status_code != 200:
            logging.error(f"Error fetching apps from leo: ${response.text}")
            return ""
        logging.info(f"Successfully retrieved details.")
        response = json.loads(response.text)
        logging.debug(response)

        #TODO have i covered all cases?
        for entries in response:
            if entries['appType'] == app_type and entries['proxyUrls'][app] is not None:
                logging.debug(entries['status'])
                if(entries['status'] == "PROVISIONING"):
                    logging.info(f"{app} is still provisioning")
                    time.sleep(30)
                elif entries['status'] == 'ERROR':
                    logging.error(f"{app} is in ERROR state. Quitting.")
                    return ""
                else:
                    return entries['proxyUrls'][app]

# UPLOAD DATA TO WORSPACE DATA SERVICE IN A WORKSPACE
def upload_wds_data(wds_url, current_workspaceId, tsv_file_name, recordName, azure_token):
    version="v0.2"
    api_client = wds_client.ApiClient(header_name='Authorization', header_value="Bearer " + azure_token)
    api_client.configuration.host = wds_url
    
    # records client is used to interact with Records in the data table
    records_client = wds_client.RecordsApi(api_client)
    # data to upload to wds table
    logging.info("uploading to wds")
    # Upload entity to workspace data table with name "testType_uploaded"
    response = records_client.upload_tsv(current_workspaceId, version, recordName, tsv_file_name)
    logging.debug(response)

# KICK OFF A WORKFLOW INSIDE A WORKSPACE
def submit_workflow_assemble_refbased(workspaceId, dataFile, azure_token):
    cbas_url = get_app_url(workspaceId, "cbas", azure_token)
    logging.debug(cbas_url)
    #open text file in read mode
    text_file = open(dataFile, "r")
    request_body2 = text_file.read();
    text_file.close()
    
    uri = f"{cbas_url}/api/batch/v1/run_sets"
    
    headers = {"Authorization": azure_token,
               "accept": "application/json", 
              "Content-Type": "application/json"}
    
    response = requests.post(uri, data=request_body2, headers=headers)
    # example of what it returns: {'run_set_id': 'cdcdc570-f6f3-4425-9404-4d70cd74ce2a', 'runs': [{'run_id': '0a72f308-4931-436b-bbfe-55856f7c1a39', 'state': 'UNKNOWN', 'errors': 'null'}, {'run_id': 'eb400221-efd7-4e1a-90c9-952f32a10b60', 'state': 'UNKNOWN', 'errors': 'null'}], 'state': 'RUNNING'}
    logging.debug(response.json())

def clone_workspace(billing_project_name, workspace_name, header):
    api_call2 = f"{rawls_url}/api/workspaces/{billing_project_name}/{workspace_name}/clone";
    request_body = {
        "namespace": billing_project_name,  # Billing project name
        "name": f"{workspace_name} clone-{''.join(random.choices(string.ascii_lowercase, k=3))}",  # workspace name
        "attributes": {}};

    logging.info(f"cloning workspace {workspace_name}")
    response = requests.post(url=api_call2, json=request_body, headers=header)

    # example json that is returned by request: 'attributes': {}, 'authorizationDomain': [], 'bucketName': '', 'createdBy': 'yulialovesterra@gmail.com', 'createdDate': '2023-08-03T20:10:59.116Z', 'googleProject': '', 'isLocked': False, 'lastModified': '2023-08-03T20:10:59.116Z', 'name': 'api-workspace-1', 'namespace': 'yuliadub-test2', 'workspaceId': 'ac466322-2325-4f57-895d-fdd6c3f8c7ad', 'workspaceType': 'mc', 'workspaceVersion': 'v2'}
    json2 = response.json()
    logging.debug(json2)
    return json2["workspaceId"]

def check_wds_data(wds_url, workspaceId, recordName, azure_token):
    version = "v0.2"
    api_client = wds_client.ApiClient(header_name='Authorization', header_value="Bearer " + azure_token)
    api_client.configuration.host = wds_url

    schema_client = wds_client.SchemaApi(api_client)

    logging.info("verifying data was cloned")
    response = schema_client.describe_record_type(workspaceId, version, recordName);
    assert response.name == recordName, "Name does not match"
    assert response.count == 2504, "Count does not match"


def delete_workspace():
    # todo
    return ""
