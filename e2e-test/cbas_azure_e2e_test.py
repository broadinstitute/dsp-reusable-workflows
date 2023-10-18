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
LOG_FORMAT = "%(asctime)s %(levelname)-6s %(message)s"
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
        logging.error(response.text)
        exit(1)
    logging.info(f"Successfully uploaded data to WDS. Response: {response.json()}")


# Create no-tasks-workflow method in CBAS
def create_cbas_method(cbas_url, workspace_id):
    method_name = "no-tasks-workflow"
    #TODO: change branch to main
    request_body = {
        "method_name": method_name,
        "method_source": "GitHub",
        "method_url": "https://raw.githubusercontent.com/broadinstitute/dsp-reusable-workflows/sps_cbas_e2e_test/e2e-test/resources/cbas/no-tasks-workflow.wdl",
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
        logging.error(response.text)
        exit(1)
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
        logging.error(response.text)
        exit(1)
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
        logging.error(response.text)
        exit(1)
    logging.info(f"Successfully submitted workflow. Response: {response.json()}")

    response = json.loads(response.text)
    return response['run_set_id']


# Check if outputs were written back to WDS for given record
def check_outputs_data(wds_url, workspace_id, record_type, record_name):
    uri = f"{wds_url}/{workspace_id}/records/v0.2/{record_type}/{record_name}"
    headers = {"Authorization": f"Bearer {azure_token}"}

    response = requests.get(uri, headers=headers)

    status_code = response.status_code
    if status_code != 200:
        logging.error(response.text)
        exit(1)
    logging.info(f"Successfully retrieved record details for record '{record_name}' of type '{record_type}'")

    response = json.loads(response.text)

    attributes = response['attributes']

    logging.info("Checking that output attributes exist in record...")
    if 'team' in attributes and attributes['team'] == "Guardians of the Galaxy" and 'rank' in attributes and attributes['rank'] == "Captain":
        logging.info("Outputs were successfully written back to WDS")
    else:
        logging.error("Outputs were not written back to WDS")
        exit(1)


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
        logging.error(response.text)
        exit(1)

    response = json.loads(response.text)
    if response['run_sets'][0]['state'] != 'COMPLETE':
        logging.error(f"Submission '{run_set_id}' not in 'COMPLETE' state. Current state: {response['run_sets'][0]['state']}.")
        exit(1)

    logging.info(f"Submission '{run_set_id}' status: COMPLETE.")


# ---------------------- Start Workflows Azure E2E test ----------------------
logging.info("Starting Workflows Azure E2E test...")

# Create workspace
logging.info("Creating workspace...")
workspace_id, workspace_name = create_workspace(billing_project_name, azure_token, rawls_url)

# Create WORKFLOWS_APP and CROMWELL_RUNNER apps in workspace
create_app(workspace_id, leo_url, 'WORKFLOWS_APP', 'WORKSPACE_SHARED', azure_token)
create_app(workspace_id, leo_url, 'CROMWELL_RUNNER_APP', 'USER_PRIVATE', azure_token)

# sleep for 5 minutes to allow workspace to provision and apps to start up
logging.info("Sleeping for 5 minutes to allow workspace to provision and apps to start up...")
time.sleep(5 * 60)

# Upload data to workspace
# check that WDS is ready; if not exit the test after 10 minutes of polling
logging.info(f"Checking to see if WDS app is ready to upload data for workspace {workspace_id}...")
wds_url = poll_for_app_url(workspace_id, 'WDS', 'wds', azure_token, leo_url)
if wds_url == "":
    logging.error(f"WDS app not ready or errored out for workspace {workspace_id}")
    exit(1)
upload_wds_data_using_api(wds_url, workspace_id, "e2e-test/resources/cbas/cbas-e2e-test-data.tsv", "test-data")

# check that CBAS is ready; if not exit the test after 10 minutes of polling
logging.info(f"Checking to see if WORKFLOWS app is ready in workspace {workspace_id}...")
cbas_url = poll_for_app_url(workspace_id, 'WORKFLOWS_APP', 'cbas', azure_token, leo_url)
if cbas_url == "":
    logging.error(f"WORKFLOWS app not ready or errored out for workspace {workspace_id}")
    exit(1)

# check that Cromwell Runner is ready; if not exit the test after 10 minutes of polling
logging.info(f"Checking to see if CROMWELL_RUNNER app is ready in workspace {workspace_id}...")
cromwell_url = poll_for_app_url(workspace_id, 'CROMWELL_RUNNER_APP', 'cromwell-runner', azure_token, leo_url)
if cromwell_url == "":
    logging.error(f"CROMWELL_RUNNER app not ready or errored out for workspace {workspace_id}")
    exit(1)

# create a new method
method_id = create_cbas_method(cbas_url, workspace_id)
method_version_id = get_method_version_id(cbas_url, method_id)

# submit workflow to CBAS
run_set_id = submit_no_tasks_workflow(cbas_url, method_version_id)

# sleep for 2 minutes to allow submission to finish
logging.info("Sleeping for 2 minutes to allow submission to finish and outputs to be written to WDS...")
time.sleep(2 * 60)

# without polling CBAS, check if outputs were written back to WDS
# we don't poll CBAS first to check that the callback API is working
logging.info("Checking to see outputs were successfully written back to WDS...")
check_outputs_data(wds_url, workspace_id, 'test-data', '89P13')

# check submission status
logging.info("Checking submission status...")
check_submission_status(cbas_url, method_id, run_set_id)

# delete workspace and apps
# TODO: Enable workspace deletion check after bug https://broadworkbench.atlassian.net/browse/WOR-1309 is complete
logging.info("Starting workspace deletion...")
delete_workspace(billing_project_name, workspace_name, rawls_url, azure_token)

logging.info("Test successfully completed. Exiting.")
