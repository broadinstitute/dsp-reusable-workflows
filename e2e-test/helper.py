
import json
import uuid
import random
import string
import wds_client
import requests
import time
import logging
import os

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
    workspace_name = workspace_name if workspace_name else f"api-workspace-{''.join(random.choices(string.ascii_lowercase, k=5))}"
    request_body= {
      "namespace": billing_project_name, # Billing project name
      "name": workspace_name,
      "attributes": {}};

    logging.debug(f"Creating workspace with name {workspace_name}")
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
        start_cbas_url = f"{leo_url}/api/apps/v2/{data['workspaceId']}/terra-app-{str(uuid.uuid4())}";
        logging.debug(f"start_cbas_url: {start_cbas_url}")
        request_body2 = {
          "appType": "CROMWELL"
        } 
        
        cbas_response = requests.post(url=start_cbas_url, json=request_body2, headers=header)
        # will return 202 or error
        logging.debug(cbas_response)

    return data['workspaceId'], data['name'], data["namespace"]

# GET WDS OR CROMWELL ENDPOINT URL FROM LEO
def poll_for_app_url(workspaceId, app, azure_token):
    """"Get url for wds/cbas."""
    leo_get_app_api = f"{leo_url}/api/apps/v2/{workspaceId}?includeDeleted=false"
    headers = {"Authorization": "Bearer " + azure_token,
               "accept": "application/json"}

    app_type = "CROMWELL" if app != 'wds' else app.upper();
    logging.info(f"App type: {app_type}")

    # prevent infinite loop
    poll_count = 20 # 30s x 20 = 10 min

    while poll_count > 0:
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
        poll_count -= 1

    logging.error(f"App still provisioning or missing after 10 minutes, quitting")
    return ""

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


def output_message(msg, type=None):
    current_time = time.strftime("%H:%M:%S", time.localtime())
    msg = f'{current_time} - {msg}'
    if type == 'DEBUG':
        logging.debug(msg)
    elif type == 'ERROR':
        logging.error(msg)
    else:
        logging.info(msg)

def handle_failed_request(response, msg, status_code=200):
    if(response.status_code != status_code):
        raise Exception(f'{response.status_code} - {msg}\n{response.text}')
    
def submit_hello_world_to_cromwell(app_url, workflow_test_name, bearer_token):
    absolute_file_path = os.path.dirname(__file__)
    workflow_source_path = os.path.join(absolute_file_path, './resources/cromwell_workflow_files/hello.wdl')
    workflow_inputs_path = os.path.join(absolute_file_path, './resources/cromwell_workflow_files/hello.inputs')
    workflow_endpoint = f'{app_url}/api/workflows/v1'
    headers = {"Authorization": f'Bearer {bearer_token}',
              "accept": "application/json",
    }
    with open(workflow_source_path, 'rb') as hello_wdl:
        with open(workflow_inputs_path, 'rb') as hello_inputs:
            files = {
                'workflowSource': ('hello.wdl', hello_wdl, 'application/octet-stream'),
                'workflowInputs': ('hello.inputs', hello_inputs, 'application/octet-stream'),
                'workflowType': 'WDL',
                'workflowTypeVersion': '1.0'
            }
            response = requests.post(workflow_endpoint, headers=headers, files=files)
            handle_failed_request(response, f"Error submitting workflow to Cromwell for {workflow_test_name}", 201)
            output_message(response.json(), "DEBUG")
            return response.json()
        
def get_workflow_information(app_url, workflow_id, bearer_token):
    workflow_endpoint = f'{app_url}/api/workflows/v1/{workflow_id}/status'
    headers = {"Authorization": f'Bearer {bearer_token}',
              "accept": "application/json"}
    output_message(f"Fetching workflow status for {workflow_id}")
    response = requests.get(workflow_endpoint, headers=headers)
    handle_failed_request(response, f"Error fetching workflow metadata for {workflow_id}")
    output_message(response.json(), "DEBUG")
    return response.json()

# workflow_ids is a deque of workflow ids
def get_completed_workflow(bearer_token, app_url, workflow_ids, max_retries=4, sleep_timer=60 * 2):
    success_statuses = ['Succeeded']
    throw_exception_statuses = ['Aborted', 'Failed']
    
    current_running_workflow_count = 0
    while workflow_ids:
        if max_retries == 0:
            raise Exception(f"Workflow(s) did not finish running within retry window ({max_retries} retries)")
        
        workflow_id = workflow_ids.pop()
        workflow_metadata = get_workflow_information(app_url, workflow_id, bearer_token)
        workflow_status = workflow_metadata['status']

        if(workflow_status in throw_exception_statuses):
            raise Exception(f"Exception raised: Workflow {workflow_id} reporting {workflow_status} status")
        if workflow_status in success_statuses:
            output_message(f"{workflow_id} finished running. Status: {workflow_metadata['status']}")
        else:
            workflow_ids.appendleft(workflow_id)
            current_running_workflow_count += 1
        if current_running_workflow_count == len(workflow_ids):
            if current_running_workflow_count == 0:
                output_message("Workflow(s) finished running")
            else:
                # Reset current count to 0 for next retry
                # Decrement max_retries by 1
                # Wait for sleep_timer before checking workflow statuses again (adjust value as needed)
                output_message(f"These workflows have yet to return a completed status: [{', '.join(workflow_ids)}]")
                max_retries -= 1
                current_running_workflow_count = 0
                time.sleep(sleep_timer)
    output_message("Workflow(s) submission and completion successful")

def delete_workspace(workspace_namespace, workspace_name, bearer_token):
    if workspace_namespace and workspace_name:
        delete_workspace_url = f"{rawls_url}/api/workspaces/v2/{workspace_namespace}/{workspace_name}"
        headers = {"Authorization": f'Bearer {bearer_token}',
                   "accept": "application/json"}
        # First call to initiate workspace deletion
        response = requests.delete(url=delete_workspace_url, headers=headers)
        output_message(response.text, "DEBUG")                              
        handle_failed_request(response, f"Error deleting workspace {workspace_name} - {workspace_namespace}", 202)
        output_message(f"Workspace {workspace_name} - {workspace_namespace} delete request submitted")
       
        # polling to ensure that workspace is deleted (which takes about 5ish minutes)
        is_workspace_deleted = False
        token_expired = False
        while not is_workspace_deleted and not token_expired:
            time.sleep(2 * 60)
            get_workspace_url = f"{rawls_url}/api/workspaces/{workspace_namespace}/{workspace_name}"
            polling_response = requests.get(url=get_workspace_url, headers=headers)
            polling_status_code = polling_response.status_code
            output_message(f"Polling GET WORKSPACE - {polling_status_code}")
            if polling_status_code == 200:
                output_message(f"Workspace {workspace_name} - {workspace_namespace} is still active")
            elif polling_status_code == 404:
                is_workspace_deleted = True
                output_message(f"Workspace {workspace_name} - {workspace_namespace} is deleted")
            elif polling_status_code == 401:
                token_expired = True
            else:
                output_message(f"Unexpected status code {polling_status_code} received\n{polling_response.text}", "ERROR")
                raise Exception(polling_response.text)
        if token_expired:
            raise Exception(f"Workspace {workspace_name} was not deleted within bearer token lifespan")
        