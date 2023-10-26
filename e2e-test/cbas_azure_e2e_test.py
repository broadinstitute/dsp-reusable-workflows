from workspace_helper import create_workspace, delete_workspace
from app_helper import create_app, poll_for_app_url
import requests
import os
import json
import random
import string
import uuid
import time
import logging


# Setup configuration
# These values should be injected into the environment before setup
azure_token = os.environ.get("AZURE_TOKEN")
bee_name = os.environ.get("BEE_NAME")
billing_project_name = os.environ.get("BILLING_PROJECT_NAME")

rawls_url = f"https://rawls.{bee_name}.bee.envs-terra.bio"
leo_url = f"https://leonardo.{bee_name}.bee.envs-terra.bio"

# configure logging format
LOG_FORMAT = "%(asctime)s %(levelname)-8s %(message)s"
LOG_LEVEL = "INFO"
LOG_DATEFORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(
    format=LOG_FORMAT,
    level=getattr(logging, LOG_LEVEL),
    datefmt=LOG_DATEFORMAT,
)


# Upload data to WDS using APIs
def upload_wds_data_using_api(wds_url, workspace_id, tsv_file_name, record_name):
    #open TSV file in read mode
    with open(tsv_file_name) as tsv_file:
        request_file = tsv_file.read()

    uri = f"{wds_url}/{workspace_id}/tsv/v0.2/{record_name}"
    headers = {"Authorization": f"Bearer {azure_token}"}

    response = requests.post(uri, files={'records':request_file}, headers=headers)

    status_code = response.status_code

    if status_code != 200:
        raise Exception(response.text)

    logging.info(f"Successfully uploaded data to WDS. Response: {response.json()}")


# Create no-tasks-workflow method in CBAS
def create_cbas_method(cbas_url, workspace_id):
    method_name = "no-tasks-workflow"
    request_body = {
        "method_name": method_name,
        "method_source": "GitHub",
        "method_url": "https://raw.githubusercontent.com/broadinstitute/dsp-reusable-workflows/main/e2e-test/resources/cbas/no-tasks-workflow.wdl",
        "method_version": "develop"
    }

    uri = f"{cbas_url}/api/batch/v1/methods"
    headers = {
        "Authorization": f"Bearer {azure_token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    response = requests.post(uri, json=request_body, headers=headers)
    status_code = response.status_code

    if status_code != 200:
        raise Exception(response.text)

    logging.info(f"Successfully created method {method_name} for workspace {workspace_id}")
    response = json.loads(response.text)

    return response['method_id']


# Get method_version_id for a given method
def get_method_version_id(cbas_url, method_id):
    uri = f"{cbas_url}/api/batch/v1/methods?method_id={method_id}"
    headers = {
        "Authorization": f"Bearer {azure_token}",
        "accept": "application/json"
    }

    response = requests.get(uri, headers=headers)
    status_code = response.status_code

    if status_code != 200:
        raise Exception(response.text)

    logging.info(f"Successfully retrieved method details for method ID {method_id}")
    response = json.loads(response.text)

    # the method version we want should be the first element in the array
    return response['methods'][0]['method_versions'][0]['method_version_id']


# Submit no-tasks-workflow to CBAS
def submit_no_tasks_workflow(cbas_url, method_version_id):
    uri = f"{cbas_url}/api/batch/v1/run_sets"
    headers = {
        "Authorization": f"Bearer {azure_token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    #open text file in read mode
    with open("e2e-test/resources/cbas/submit_workflow_body.json") as request_body_file:
        request_body = request_body_file.read().replace("{METHOD_VERSION_ID}", method_version_id)

    logging.info(f"Submitting workflow to CBAS...")

    response = requests.post(uri, data=request_body, headers=headers)

    status_code = response.status_code
    if status_code != 200:
        raise Exception(response.text)

    logging.info(f"Successfully submitted workflow. Response: {response.json()}")

    response = json.loads(response.text)
    return response['run_set_id']


# Poll to check if outputs were written back to WDS for given record
def poll_for_outputs_data(wds_url, workspace_id, record_type, record_name):
    wds_records_url = f"{wds_url}/{workspace_id}/records/v0.2/{record_type}/{record_name}"
    headers = {"Authorization": f"Bearer {azure_token}"}

    # prevent infinite loop
    poll_count = 20 # 30s x 20 = 10 min

    # poll every 30 seconds to check if outputs have been written back to WDS
    while poll_count > 0:
        response = requests.get(wds_records_url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Error fetching details for record '{record_name}' of type '{record_type}'. Error: {response.text}")

        logging.info(f"Successfully retrieved details for record '{record_name}' of type '{record_type}'")

        response = json.loads(response.text)
        attributes = response['attributes']

        if 'team' in attributes and attributes['team'] == "Guardians of the Galaxy" and 'rank' in attributes and attributes['rank'] == "Captain":
            logging.info("Outputs were successfully written back to WDS")
            return
        else:
            logging.info("Outputs haven't been written back to WDS yet. Sleeping for 30 seconds")
            time.sleep(30)

        poll_count -= 1

    raise Exception(f"Outputs were not written back to WDS after polling for 10 minutes")


# Check submission is in COMPLETE state
def check_submission_status(cbas_url, method_id, run_set_id):
    uri = f"{cbas_url}/api/batch/v1/run_sets?method_id={method_id}"
    headers = {
        "Authorization": f"Bearer {azure_token}",
        "accept": "application/json"
    }

    response = requests.get(uri, headers=headers)
    status_code = response.status_code

    if status_code != 200:
        raise Exception(response.text)

    response = json.loads(response.text)
    if response['run_sets'][0]['state'] != 'COMPLETE':
        raise Exception(f"Submission '{run_set_id}' not in 'COMPLETE' state. Current state: {response['run_sets'][0]['state']}.")

    logging.info(f"Submission '{run_set_id}' status: COMPLETE")


def test_cleanup(workspace_name):
    try:
        delete_workspace(billing_project_name, workspace_name, rawls_url, azure_token)
        logging.info("Workspace cleanup complete")
    # Catch the exception and continue with the test since we don't want cleanup to affect the test results.
    # We can assume that Janitor will clean up the workspace if the test fails
    # TODO: Instead of catching exception and continuing with test, the script should fail the test once
    #       https://broadworkbench.atlassian.net/browse/WOR-1309 is fixed
    except Exception as e:
        logging.warning(f"Error cleaning up workspace, test script will continue. Error details: {e}")


# ---------------------- Start Workflows Azure E2E test ----------------------
found_exception = False
try:
    logging.info("Starting Workflows Azure E2E test...")

    # Create workspace
    logging.info("Creating workspace...")
    workspace_id, workspace_name = create_workspace(billing_project_name, azure_token, rawls_url)

    # sleep for 1 minute to allow apps that auto-launch to start provisioning
    logging.info("Sleeping for 1 minute to allow apps that auto-launch to start provisioning...")
    time.sleep(60)

    # After "Multi-user Workflow: Auto app start up" phase is completed, WORKFLOWS_APP will be launched
    # automatically at workspace creation time (similar to WDS). So to prevent test failures and errors
    # (until script is updated) when the code for that phase is released in dev, we check here if the WORKFLOWS_APP
    # is already deployed before manually creating it. `poll_for_app_url` before starting to poll checks & returns
    # "" if the app was never deployed
    # Note: After "Multi-user Workflow: Auto app start up" phase is completed, update the script and remove
    #       these 4 lines as we already poll for CBAS proxy url down below
    logging.info("Checking to see if WORKFLOWS_APP was deployed...")
    cbas_url = poll_for_app_url(workspace_id, 'WORKFLOWS_APP', 'cbas', azure_token, leo_url)
    if cbas_url == "":
        create_app(workspace_id, leo_url, 'WORKFLOWS_APP', 'WORKSPACE_SHARED', azure_token)

    # Since CROMWELL_RUNNER app needs the `cromwellmetadata` database available before it can be deployed,
    # wait for WORKFLOWS app to be in Running state before deploying CROMWELL_RUNNER app.
    # Check that CBAS is ready; if not fail the test after 10 minutes of polling
    logging.info(f"Polling to check if WORKFLOWS app is ready in workspace {workspace_id}...")
    cbas_url = poll_for_app_url(workspace_id, 'WORKFLOWS_APP', 'cbas', azure_token, leo_url)
    if cbas_url == "":
        raise Exception(f"WORKFLOWS app not ready or errored out for workspace {workspace_id}")

    # Create CROMWELL_RUNNER app in workspace
    create_app(workspace_id, leo_url, 'CROMWELL_RUNNER_APP', 'USER_PRIVATE', azure_token)

    # check that Cromwell Runner is ready; if not fail the test after 10 minutes of polling
    logging.info(f"Polling to check if CROMWELL_RUNNER app is ready in workspace {workspace_id}...")
    cromwell_url = poll_for_app_url(workspace_id, 'CROMWELL_RUNNER_APP', 'cromwell-runner', azure_token, leo_url)
    if cromwell_url == "":
        raise Exception(f"CROMWELL_RUNNER app not ready or errored out for workspace {workspace_id}")

    # check that WDS is ready; if not fail the test after 10 minutes of polling
    logging.info(f"Polling to check if WDS app is ready to upload data for workspace {workspace_id}...")
    wds_url = poll_for_app_url(workspace_id, 'WDS', 'wds', azure_token, leo_url)
    if wds_url == "":
        raise Exception(f"WDS app not ready or errored out for workspace {workspace_id}")

    # upload data to workspace
    upload_wds_data_using_api(wds_url, workspace_id, "e2e-test/resources/cbas/cbas-e2e-test-data.tsv", "test-data")

    # create a new method
    method_id = create_cbas_method(cbas_url, workspace_id)
    method_version_id = get_method_version_id(cbas_url, method_id)

    # submit workflow to CBAS
    run_set_id = submit_no_tasks_workflow(cbas_url, method_version_id)

    # Without polling CBAS, check if outputs were written back to WDS. We don't poll CBAS first to verify
    # that the callback API is working
    # Note: When "Multi-user Workflows: Auto app start up" phase is completed, CROMWELL_RUNNER app will
    #       be deployed automatically by WORKFLOWS app. As a result, a submission would take time to reach a
    #       terminal state as CROMWELL_RUNNER app would take a while to provision and then run the workflow.
    #       To avoid test failures when this happens we poll for 10 minutes to check if outputs are written back to WDS.
    logging.info("Polling to check if outputs were successfully written back to WDS...")
    poll_for_outputs_data(wds_url, workspace_id, 'test-data', '89P13')

    # check submission status
    logging.info("Checking submission status...")
    check_submission_status(cbas_url, method_id, run_set_id)
except Exception as e:
    logging.error(f"Exception(s) occurred during test. Details: {e}")
    found_exception = True
finally:
    # delete workspace and apps
    logging.info("Starting workspace cleanup...")
    test_cleanup(workspace_name)

    # Use exit(1) so that GHA will fail if an exception was found during the test
    if(found_exception):
        logging.error("Exceptions found during test run. Test failed")
        exit(1)
    else:
        logging.info("Test completed successfully")
