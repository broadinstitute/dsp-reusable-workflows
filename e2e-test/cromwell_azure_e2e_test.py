import requests
import logging
import os
import time
from collections import deque
from workspace_helper import create_workspace, delete_workspace
from app_helper import create_app, poll_for_app_url

# configure logging format
LOG_FORMAT = "%(asctime)s %(levelname)-8s %(message)s"
LOG_LEVEL = "INFO"
LOG_DATEFORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(
    format=LOG_FORMAT,
    level=getattr(logging, LOG_LEVEL),
    datefmt=LOG_DATEFORMAT,
)

token = os.environ['BEARER_TOKEN']
bee_name = os.environ['BEE_NAME']
billing_project_name = os.environ['BILLING_PROJECT_NAME']

rawls_url = f"https://rawls.{bee_name}.bee.envs-terra.bio"
leo_url = f"https://leonardo.{bee_name}.bee.envs-terra.bio"

def perform_get(endpoint, bearer_token):
    headers = {"Authorization": f'Bearer {bearer_token}',
              "accept": "application/json"}
    response = requests.get(endpoint, headers=headers)
    if(response.status_code != 200):
        msg = f"Error response from {endpoint}"
        raise Exception(f'{response.status_code} - {msg}\n{response.text}')
    return response.json()

def submit_hello_world_to_cromwell(app_url, workflow_test_name, bearer_token):
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
            if(response.status_code != 201):
                msg = f"Error submitting workflow to Cromwell for {workflow_test_name}"
                raise Exception(f'{response.status_code} - {msg}\n{response.text}')
            logging.debug(response.json())
            return response.json()
        
def get_workflow_status(app_url, workflow_id, bearer_token):
    workflow_endpoint = f'{app_url}/api/workflows/v1/{workflow_id}/status'
    logging.info(f"Fetching workflow status for {workflow_id}")
    return perform_get(workflow_endpoint, bearer_token)

def get_workflow_metadata(app_url, workflow_id, bearer_token):
    workflow_endpoint = f'{app_url}/api/workflows/v1/{workflow_id}/metadata'
    logging.info(f"Fetching workflow metadata for {workflow_id}")
    return perform_get(workflow_endpoint, bearer_token)

# workflow_ids is a deque of workflow ids
def get_completed_workflow(app_url, workflow_ids, bearer_token, max_attempts=6, sleep_timer=60 * 2):
    success_statuses = ['Succeeded']
    throw_exception_statuses = ['Aborted', 'Failed']

    remaining_attempts = max_attempts
    
    current_running_workflow_count = 0
    while workflow_ids:
        if remaining_attempts == 0:
            raise Exception(f"Workflow(s) did not finish running within retry window ({max_attempts} attempts)")
        
        workflow_id = workflow_ids.pop()
        workflow_status_json = get_workflow_status(app_url, workflow_id, bearer_token)
        logging.info(f"Status response is: {workflow_status_json}")
        workflow_status = workflow_status_json['status']

        if(workflow_status in throw_exception_statuses):
            metadata = get_workflow_metadata(app_url, workflow_id, bearer_token)
            logging.info(metadata)
            raise Exception(f"Exception raised: Workflow {workflow_id} reporting {workflow_status} status")
        if workflow_status in success_statuses:
            logging.info(f"{workflow_id} finished running. Status: {workflow_status}")
        else:
            workflow_ids.appendleft(workflow_id)
            current_running_workflow_count += 1
        if current_running_workflow_count == len(workflow_ids):
            if current_running_workflow_count == 0:
                logging.info("Workflow(s) finished running")
            else:
                # Reset current count to 0 for next retry
                # Decrement remaining_attempts by 1
                # Wait for sleep_timer before checking workflow statuses again (adjust value as needed)
                logging.info(f"These workflows have yet to return a completed status: [{', '.join(workflow_ids)}]")
                remaining_attempts -= 1
                current_running_workflow_count = 0
                time.sleep(sleep_timer)
    logging.info("Workflow(s) submission and completion successful")
        
def test_cleanup(workspace_namespace, workspace_name, bearer_token):
    try:
        delete_workspace(workspace_namespace, workspace_name, rawls_url, bearer_token)
        logging.info("Workspace cleanup complete")
    # Catch the exeception and continue with the test since we don't want cleanup to affect the test results
    # We can assume that Janitor will clean up the workspace if the test fails
    except Exception as e:
        logging.info("Error cleaning up workspace, test script will continue")
        logging.info(f'Exception details below:\n{e}')
        
def main():
    workspace_name = ""
    workspace_id = ""
    found_exception = False
    
    # Sleep timers for various steps in the test
    provision_sleep_timer = 60 * 5
    workflow_run_sleep_timer = 60 * 5
    workflow_status_sleep_timer = 60 * 2
    
    try:
        (workspace_id, workspace_name) = create_workspace(billing_project_name, token, rawls_url)

        create_app(workspace_id, leo_url, 'WORKFLOWS_APP', 'WORKSPACE_SHARED', token)
        workflow_app_url = poll_for_app_url(workspace_id, 'WORKFLOWS_APP', 'cbas', token, leo_url)
        
        if not workflow_app_url:
            raise Exception("Prerequisite workflow app URL could not be found.")
        
        create_app(workspace_id, leo_url, 'CROMWELL_RUNNER_APP', 'USER_PRIVATE', token)
        app_url = poll_for_app_url(workspace_id, 'CROMWELL_RUNNER_APP', 'cromwell-runner', token, leo_url)

        if not app_url:
            raise Exception("App URL could not be found.")

        # This chunk of code only executes one workflow
        # Would like to modify this down the road to execute and store references for multiple workflows
        workflow_response = submit_hello_world_to_cromwell(app_url, "Run Workflow Test", token)
        logging.info(f'Executing sleep for {workflow_run_sleep_timer} seconds to allow workflow(s) to complete')
        time.sleep(workflow_run_sleep_timer)

        # This chunk of code supports checking one or more workflows
        # Probably won't require too much modification if we want to run additional submission tests
        workflow_ids = deque([workflow_response['id']])
        get_completed_workflow(app_url, workflow_ids, token, sleep_timer=workflow_status_sleep_timer)
    except Exception as e:
        logging.info(f"Exception occured during test:\n{e}")
        found_exception = True
    finally:
        test_cleanup(billing_project_name, workspace_name, token)
        # Use exit(1) so that GHA will fail if an exception was found during the test
        if(found_exception):
            logging.error("Workflow test failed due to exception(s)")
            exit(1)

if __name__ == "__main__":
    main()
