from workspace_helper import create_workspace, delete_workspace
from app_helper import poll_for_app_url
import uuid
import random
import string
import wds_client
import requests
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

# CREATE WORKSPACE ACTION WITH APP CREATION
def create_workspace_with_cromwell_app(cbas, billing_project_name, azure_token, workspace_name = ""):
    workspace_id, workspace_name = create_workspace(billing_project_name, azure_token, rawls_url, workspace_name)
    
    # enable CBAS if specified
    header = {
      "Authorization": "Bearer " + azure_token,
      "accept": "application/json"
    }

    if cbas is True:
        logging.info(f"Enabling CBAS for workspace {workspace_id}")
        start_cbas_url = f"{leo_url}/api/apps/v2/{workspace_id}/terra-app-{str(uuid.uuid4())}";
        logging.debug(f"start_cbas_url: {start_cbas_url}")
        request_body2 = {
          "appType": "CROMWELL"
        } 
        
        cbas_response = requests.post(url=start_cbas_url, json=request_body2, headers=header)
        # will return 202 or error
        logging.debug(cbas_response)

    return workspace_id, workspace_name

# UPLOAD DATA TO WORSPACE DATA SERVICE IN A WORKSPACE
def upload_wds_data(wds_url, current_workspaceId, tsv_file_name, recordName, azure_token):
    version="v0.2"
    api_client = wds_client.ApiClient(header_name='Authorization', header_value="Bearer " + azure_token)
    api_client.configuration.host = wds_url
    
    # records client is used to interact with Records in the data table
    records_client = wds_client.RecordsApi(api_client)

    # determine number of lines in table
    with open(tsv_file_name, 'r') as file:
        for count, line in enumerate(file):
            pass

    # data to upload to wds table
    logging.info("uploading to wds")
    # Upload entity to workspace data table with name "testType_uploaded"
    response = records_client.upload_tsv(current_workspaceId, version, recordName, tsv_file_name)
    logging.debug(response)
    assert response.records_modified == count, f"Uploading to wds failed: {response.reason}"
    # return true if upload succeeded
    return response.records_modified == count


# KICK OFF A WORKFLOW INSIDE A WORKSPACE
def submit_workflow_assemble_refbased(workspaceId, dataFile, azure_token):
    cbas_url = poll_for_app_url(workspaceId, "CROMWELL", "cbas", azure_token, leo_url)
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

def clone_workspace(billing_project_name, workspace_name, header, delete_created_workspace, azure_token):
    clone_workspace_api = f"{rawls_url}/api/workspaces/{billing_project_name}/{workspace_name}/clone";
    cloneWorkspaceName =  f"{workspace_name} clone-{''.join(random.choices(string.ascii_lowercase, k=3))}";
    request_body = {
        "namespace": billing_project_name,  # Billing project name
        "name": cloneWorkspaceName,  # workspace name
        "attributes": {}};

    logging.info(f"cloning workspace {workspace_name}")
    response = requests.post(url=clone_workspace_api, json=request_body, headers=header)
    assert response.status_code == 201, f"Cloning {workspace_name} failed: {response.reason}"
    # example json that is returned by request: 'attributes': {}, 'authorizationDomain': [], 'bucketName': '', 'createdBy': 'yulialovesterra@gmail.com', 'createdDate': '2023-08-03T20:10:59.116Z', 'googleProject': '', 'isLocked': False, 'lastModified': '2023-08-03T20:10:59.116Z', 'name': 'api-workspace-1', 'namespace': 'yuliadub-test2', 'workspaceId': 'ac466322-2325-4f57-895d-fdd6c3f8c7ad', 'workspaceType': 'mc', 'workspaceVersion': 'v2'}
    clone_response_json = response.json()
    logging.debug(clone_response_json)
    
    if delete_created_workspace:
        test_cleanup(billing_project_name, cloneWorkspaceName, azure_token)
    
    return clone_response_json["workspaceId"]

def check_wds_data(wds_url, workspaceId, recordName, azure_token):
    version = "v0.2"
    api_client = wds_client.ApiClient(header_name='Authorization', header_value="Bearer " + azure_token)
    api_client.configuration.host = wds_url

    schema_client = wds_client.SchemaApi(api_client)

    logging.info("verifying data was cloned")
    response = schema_client.describe_record_type(workspaceId, version, recordName);
    assert response.name == recordName, "Name does not match"
    assert response.count == 2506, "Count does not match"

def test_cleanup(billing_project_name, workspace_name, azure_token):
    try:
        delete_workspace(billing_project_name, workspace_name, rawls_url, azure_token)
        logging.info("Workspace cleanup complete")
    # Catch the exception and continue with the test since we don't want cleanup to affect the test results.
    except Exception as e:
        logging.warning(f"Error cleaning up workspace, test script will continue. Error details: {e}")
