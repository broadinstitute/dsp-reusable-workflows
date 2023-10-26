import requests
import json
import random
import string
import time
import logging


# CREATE WORKSPACE ACTION
def create_workspace(billing_project_name, azure_token, rawls_url, workspace_name = ""):
    # create a new workspace, need to have attributes or api call doesnt work
    rawls_workspace_api = f"{rawls_url}/api/workspaces"
    workspace_name = workspace_name if workspace_name else f"e2e-test-api-workspace-{''.join(random.choices(string.ascii_lowercase, k=5))}"
    logging.info(f"Creating workspace {workspace_name} in {billing_project_name}")
    request_body= {
        "namespace": billing_project_name,
        "name": workspace_name,
        "attributes": {}
    }
    header = {
        "Authorization": "Bearer " + azure_token,
        "accept": "application/json"
    }

    workspace_response = requests.post(url=rawls_workspace_api, json=request_body, headers=header)
    if workspace_response.status_code != 201:
        raise Exception(f"Error creating workspace: {workspace_response.text}")

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
    workspace_response_json = workspace_response.json()
    data = json.loads(json.dumps(workspace_response_json))

    workspace_id = data['workspaceId']
    logging.info(f"Successfully started workspace creation for '{workspace_name}' in billing project '{billing_project_name}'. Workspace ID returned: {workspace_id}")

    return workspace_id, data['name']


# DELETE WORKSPACE ACTION
def delete_workspace(billing_project_name, workspace_name, rawls_url, azure_token):
    delete_workspace_url = f"{rawls_url}/api/workspaces/v2/{billing_project_name}/{workspace_name}"
    headers = {
        "Authorization": "Bearer " + azure_token,
        "accept": "application/json"
    }

    delete_response = requests.delete(url=delete_workspace_url, headers=headers)
    if delete_response.status_code != 202:
        raise Exception(f"Error submitting deletion workspace request: {delete_response.text}")

    logging.info(f"Successfully submitted deletion request for workspace '{workspace_name}' in billing project '{billing_project_name}'. Response: {delete_response.text}")

    # sleep for 2 minutes
    logging.info("Sleeping for 2 minutes before polling for workspace status...")
    time.sleep(2 * 60)

    # prevent infinite loop
    poll_count = 16 # 30s x 16 = 8 min

    # poll every 30s to check if workspace was deleted
    workspace_status_url = f"{rawls_url}/api/workspaces/{billing_project_name}/{workspace_name}"
    while poll_count > 0:
        response = requests.get(workspace_status_url, headers=headers)
        status_code = response.status_code

        if status_code == 200:
            logging.info(f"Workspace '{workspace_name}' in billing project '{billing_project_name}' still exists. Sleeping for 30 seconds")
            time.sleep(30)
        elif status_code == 401:
            raise Exception(f"Azure token expired.")
        elif status_code == 404:
            logging.info(f"Workspace '{workspace_name}' in billing project '{billing_project_name}' deleted successfully")
            return
        else:
            raise Exception(f"Something went wrong while workspace deletion. Received status code {status_code}. Error: {response.text}")

        poll_count -= 1

    raise Exception(f"Workspace wasn't deleted within 10 minutes.")
