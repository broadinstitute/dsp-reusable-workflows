import requests
import os
import json
import random
import string
import uuid
import time


# CREATE WORKSPACE ACTION
def create_workspace(billing_project_name, azure_token, rawls_url, workspace_name = ""):
    # create a new workspace, need to have attributes or api call doesnt work
    rawls_workspace_api = f"{rawls_url}/api/workspaces"
    workspace_name = workspace_name if workspace_name else f"api-workspace-{''.join(random.choices(string.ascii_lowercase, k=5))}"
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
    assert workspace_response.status_code == 201, f"Error creating workspace: ${workspace_response.text}"

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
    print(f"Successfully started workspace creation for '{workspace_name}' in billing project '{billing_project_name}'. Workspace ID returned: {workspace_id}")

    return workspace_id, data['name']
