
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
def create_workspace(cbas, billing_project_name, header, workspace_name = ""):
    # create a new workspace, need to have attributes or api call doesnt work
    rawls_workspace_api = f"{rawls_url}/api/workspaces";
    request_body= {
      "namespace": billing_project_name, # Billing project name
      "name": workspace_name if workspace_name else f"api-workspace-{''.join(random.choices(string.ascii_lowercase, k=5))}",
      "attributes": {}};

    workpace_response = requests.post(url=rawls_workspace_api, json=request_body, headers=header)
    logging.debug(f"url: {rawls_workspace_api}")
    logging.debug(f"response: {workpace_response}")
    logging.debug(f"request_body: {request_body}")
    assert workpace_response.status_code == 201, f"Error creating workspace: ${workpace_response.text}"
    
    # example json that is returned by request:
    # {
    #   "attributes": {},
    #   "authorizationDomain": [],
    #   "bucketName": "",
    #   "createdBy": "yulialovesterra@gmail.com",
    #   "createdDate": "2023-08-03T20:10:59.116Z",
    #   "googleProject": "",
    #   "isLocked": False,
    #   "lastModified": "2023-08-03T20:10:59.116Z",
    #   "name": "api-workspace-1",
    #   "namespace": "yuliadub-test2",
    #   "workspaceId": "ac466322-2325-4f57-895d-fdd6c3f8c7ad",
    #   "workspaceType": "mc",
    #   "workspaceVersion": "v2"
    # }
    workspace_response_json = workpace_response.json()
    logging.debug(f"json response: {workspace_response_json}")
    data = json.loads(json.dumps(workspace_response_json))
    
    logging.debug(f"data['workspaceId']:  {data['workspaceId']}")
    
    # enable CBAS if specified
    if cbas is True:
        logging.info(f"Enabling CBAS for workspace {data['workspaceId']}")
        start_cbas_api = f"{leo_url}/api/apps/v2/{data['workspaceId']}/terra-app-{str(uuid.uuid4())}";
        request_body2 = {
          "appType": "CROMWELL"
        } 
        
        cbas_response = requests.post(url=start_cbas_api, json=request_body2, headers=header)
        # will return 202 or error
        logging.debug(cbas_response)

    return data['workspaceId'], data['name']

# GET WDS OR CROMWELL ENDPOINT URL FROM LEO
def poll_for_app_url(workspaceId, app, azure_token):
    """"Get url for wds/cbas."""
    leo_get_app_api = f"{leo_url}/api/apps/v2/{workspaceId}?includeDeleted=false"
    headers = {"Authorization": "Bearer " + azure_token,
               "accept": "application/json"}

    app_type = "CROMWELL" if app != 'wds' else app.upper();
    logging.info(f"App type: {app_type}")

    #TODO: can this get into an infinite loop?
    while True:
        response = requests.get(leo_get_app_api, headers=headers)
        assert response.status_code == 200, f"Error fetching apps from leo: ${response.text}"
        logging.info(f"Successfully retrieved details.")
        response = json.loads(response.text)
        logging.debug(response)

        # Don't run in an infinite loop if you forgot to start the app/it was never created
        if app_type not in [item['appType'] for item in response]:
            print(f"{app_type} not found in apps, has it been started?")
            return ""
        for entries in response:
            if entries['appType'] == app_type:
                if entries['status'] == "PROVISIONING":
                    logging.info(f"{app} is still provisioning")
                    time.sleep(30)
                elif entries['status'] == 'ERROR':
                    logging.error(f"{app} is in ERROR state. Quitting.")
                    return ""
                elif app not in entries['proxyUrls'] or entries['proxyUrls'][app] is None:
                    logging.error(f"{app} proxyUrls not found: {entries}")
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
    assert response.records_modified == 2504, f"Uploading to wds failed: {response.reason}"


# KICK OFF A WORKFLOW INSIDE A WORKSPACE
def submit_workflow_assemble_refbased(workspaceId, dataFile, azure_token):
    cbas_url = poll_for_app_url(workspaceId, "cbas", azure_token)
    logging.debug(cbas_url)
    #open text file in read mode
    text_file = open(dataFile, "r")
    request_body = text_file.read();
    text_file.close()
    
    cbas_run_sets_api = f"{cbas_url}/api/batch/v1/run_sets"
    
    headers = {"Authorization": azure_token,
               "accept": "application/json", 
              "Content-Type": "application/json"}
    
    response = requests.post(cbas_run_sets_api, data=request_body, headers=headers)
    # example of what it returns:
    # {
    #   "run_set_id": "cdcdc570-f6f3-4425-9404-4d70cd74ce2a",
    #   "runs": [
    #     {
    #       "run_id": "0a72f308-4931-436b-bbfe-55856f7c1a39",
    #       "state": "UNKNOWN",
    #       "errors": "null"
    #     },
    #     {
    #       "run_id": "eb400221-efd7-4e1a-90c9-952f32a10b60",
    #       "state": "UNKNOWN",
    #       "errors": "null"
    #     }
    #   ],
    #   "state": "RUNNING"
    # }
    logging.debug(response.json())

def clone_workspace(billing_project_name, workspace_name, header):
    clone_workspace_api = f"{rawls_url}/api/workspaces/{billing_project_name}/{workspace_name}/clone";
    request_body = {
        "namespace": billing_project_name,  # Billing project name
        "name": f"{workspace_name} clone-{''.join(random.choices(string.ascii_lowercase, k=3))}",  # workspace name
        "attributes": {}};

    logging.info(f"cloning workspace {workspace_name}")
    response = requests.post(url=clone_workspace_api, json=request_body, headers=header)
    assert response.status_code == 201, f"Cloning {workspace_name} failed: {response.reason}"
    # example json that is returned by request: 'attributes': {}, 'authorizationDomain': [], 'bucketName': '', 'createdBy': 'yulialovesterra@gmail.com', 'createdDate': '2023-08-03T20:10:59.116Z', 'googleProject': '', 'isLocked': False, 'lastModified': '2023-08-03T20:10:59.116Z', 'name': 'api-workspace-1', 'namespace': 'yuliadub-test2', 'workspaceId': 'ac466322-2325-4f57-895d-fdd6c3f8c7ad', 'workspaceType': 'mc', 'workspaceVersion': 'v2'}
    clone_response_json = response.json()
    logging.debug(clone_response_json)
    return clone_response_json["workspaceId"]

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
