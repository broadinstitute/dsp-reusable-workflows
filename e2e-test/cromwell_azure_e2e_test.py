import requests
import os
import json
import random
import string
import uuid
import time
from collections import deque
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from helper import get_completed_workflow, submit_hello_world_to_cromwell, delete_workspace, output_message, handle_failed_request

bearer_token = os.environ['BEARER_TOKEN']
bee_name = os.environ['BEE_NAME']
billing_project_name = os.environ['BILLING_PROJECT_NAME']

rawls_url = f"https://rawls.{bee_name}.bee.envs-terra.bio"
leo_url = f"https://leonardo.{bee_name}.bee.envs-terra.bio"

def create_workspace():
   rawls_api_call = f"{rawls_url}/api/workspaces"
   request_body= {
      "namespace": billing_project_name, # Billing project name
      "name": f"api-workspace-{''.join(random.choices(string.ascii_lowercase, k=5))}", # workspace name
      "attributes": {}}
   
   create_workspace_response = requests.post(url=rawls_api_call, 
                                             json=request_body, 
                                             headers={"Authorization": f"Bearer {bearer_token}"}
   ).json()

   create_workspace_data = json.loads(json.dumps(create_workspace_response))
   workspace_id = create_workspace_data['workspaceId']

   output_message(f"Enabling Cromwell for workspace {workspace_id}")
   activate_cromwell_request = f"{leo_url}/api/apps/v2/{workspace_id}/terra-app-{str(uuid.uuid4())}"
   cromwell_request_body = {
      "appType": "CROMWELL_RUNNER"
   } 
        
   response = requests.post(url=activate_cromwell_request, json=cromwell_request_body, 
                            headers={"Authorization": f"Bearer {bearer_token}"})
   # will return 202 or error
   handle_failed_request(response, "Error activating Cromwell", 202)
   output_message("Cromwell successfully activated")
   return create_workspace_data

# GET CROMWELL ENDPOINT URL FROM LEO
def get_app_url(workspaceId, app):
    uri = f"{leo_url}/api/apps/v2/{workspaceId}?includeDeleted=false"

    headers = {"Authorization": f"Bearer {bearer_token}",
               "accept": "application/json"}

    response = requests.get(uri, headers=headers)
    status_code = response.status_code
    handle_failed_request(response, f"{status_code} - Error retrieving details for {workspaceId}", 200)
    output_message(f"Successfully retrieved details.\n{response.text}")
    response = response.json()

    app_url = ""
    app_type = "CROMWELL_RUNNER" if app != 'wds' else app.upper()
    output_message(f"App type: {app_type}")
    for entries in response: 
        if entries['appType'] == app_type and entries['proxyUrls'][app] is not None:
            if(entries['status'] == "PROVISIONING"):
                output_message(f"{app} is still provisioning")
                break
            output_message(f"App status: {entries['status']}")
            app_url = entries['proxyUrls'][app]
            break 

    if app_url is None: 
        output_message(f"{app} is missing in current workspace")
    else:
        output_message(f"{app} url: {app_url}")

    return app_url
        
def test_cleanup(workspace_namespace, workspace_name):
    try:
        delete_workspace(workspace_namespace, workspace_name)
        output_message("Workspace cleanup complete")
    # Catch the exeception and continue with the test since we don't want cleanup to affect the test results
    # We can assume that Janitor will clean up the workspace if the test fails
    except Exception as e:
        output_message("Error cleaning up workspace, test script will continue")
        output_message(f'Exception details below:\n{e}')

def main():
    workspace_namespace = ""
    workspace_name = ""
    workspace_id = ""
    found_exception = False
    
    # Sleep timers for various steps in the test
    workflow_run_sleep_timer = 60 * 5
    provision_sleep_timer = 60 * 15
    workflow_status_sleep_timer = 60 * 2
    
    try:
        created_workspace = create_workspace()
        workspace_id = created_workspace['workspaceId']
        workspace_namespace = created_workspace['namespace']
        workspace_name = created_workspace['name']
        time.sleep(provision_sleep_timer) # Added an sleep here to give the workspace time to provision
        app_url = get_app_url(workspace_id, 'cromwell')

        # This chunk of code only executes one workflow
        # Would like to modify this down the road to execute and store references for multiple workflows
        workflow_response = submit_hello_world_to_cromwell(app_url, "Run Workflow Test")
        output_message(f'Executing sleep for {workflow_run_sleep_timer} seconds to allow workflow(s) to complete')
        time.sleep(workflow_run_sleep_timer)

        # This chunk of code supports checking one or more workflows
        # Probably won't require too much modification if we want to run additional submission tests
        workflow_ids = deque([workflow_response['id']])
        get_completed_workflow(app_url, workflow_ids, workflow_status_sleep_timer)
    except Exception as e:
        output_message(f"Exception occured during test:\n{e}")
        found_exception = True
    finally:
        test_cleanup(workspace_namespace, workspace_name)
        # Use exit(1) so that GHA will fail if an exception was found during the test
        if(found_exception):
            output_message("Workflow test failed due to exception(s)", "ERROR")
            exit(1)

if __name__ == "__main__":
    main()
