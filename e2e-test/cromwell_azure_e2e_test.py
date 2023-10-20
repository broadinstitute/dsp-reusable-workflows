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
from helper import output_message, handle_failed_request
from workspace_helper import create_workspace, delete_workspace
from app_helper import create_app, poll_for_app_url

bearer_token = os.environ['BEARER_TOKEN']
bee_name = os.environ['BEE_NAME']
billing_project_name = os.environ['BILLING_PROJECT_NAME']

rawls_url = f"https://rawls.{bee_name}.bee.envs-terra.bio"
leo_url = f"https://leonardo.{bee_name}.bee.envs-terra.bio"

def submit_hello_world_to_cromwell(app_url, workflow_test_name):
    absolute_file_path = os.path.dirname(__file__)
    workflow_source_path = os.path.join(absolute_file_path, './resources/cromwell/hello.wdl')
    workflow_inputs_path = os.path.join(absolute_file_path, './resources/cromwell/hello.inputs')
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
def get_completed_workflow(app_url, workflow_ids, max_retries=4, sleep_timer=60 * 2):
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
        
def test_cleanup(workspace_namespace, workspace_name):
    try:
        delete_workspace(workspace_namespace, workspace_name, rawls_url, bearer_token)
        output_message("Workspace cleanup complete")
    # Catch the exeception and continue with the test since we don't want cleanup to affect the test results
    # We can assume that Janitor will clean up the workspace if the test fails
    except Exception as e:
        output_message("Error cleaning up workspace, test script will continue")
        output_message(f'Exception details below:\n{e}')

def main():
    workspace_name = ""
    workspace_id = ""
    found_exception = False
    
    # Sleep timers for various steps in the test
    workflow_run_sleep_timer = 60 * 5
    provision_sleep_timer = 60 * 5
    workflow_status_sleep_timer = 60 * 2
    
    try:
        (workspace_id, workspace_name) = create_workspace(billing_project_name, bearer_token, rawls_url)
        create_app(workspace_id, leo_url, 'CROMWELL_RUNNER', 'USER_PRIVATE', bearer_token)
        time.sleep(provision_sleep_timer) # Added an sleep here to give the workspace time to provision and app to start
        app_url = poll_for_app_url(workspace_id, 'CROMWELL_RUNNER', 'cromwell-runner', bearer_token, leo_url)

        # This chunk of code only executes one workflow
        # Would like to modify this down the road to execute and store references for multiple workflows
        workflow_response = submit_hello_world_to_cromwell(app_url, "Run Workflow Test")
        output_message(f'Executing sleep for {workflow_run_sleep_timer} seconds to allow workflow(s) to complete')
        time.sleep(workflow_run_sleep_timer)

        # This chunk of code supports checking one or more workflows
        # Probably won't require too much modification if we want to run additional submission tests
        workflow_ids = deque([workflow_response['id']])
        get_completed_workflow(app_url, workflow_ids, sleep_timer=workflow_status_sleep_timer)
    except Exception as e:
        output_message(f"Exception occured during test:\n{e}")
        found_exception = True
    finally:
        test_cleanup(billing_project_name, workspace_name)
        # Use exit(1) so that GHA will fail if an exception was found during the test
        if(found_exception):
            output_message("Workflow test failed due to exception(s)", "ERROR")
            exit(1)

if __name__ == "__main__":
    main()
