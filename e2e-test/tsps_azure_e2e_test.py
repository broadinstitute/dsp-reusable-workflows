from workspace_helper import create_workspace, delete_workspace
from app_helper import create_app, poll_for_app_url
import argparse
import requests
import os
import json
import random
import string
import uuid
import time
import logging

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run e2e test for TSPS/Imputation service')
    parser.add_argument('-t', '--token', required=False,
        help='token to authenticate Terra API calls')
    parser.add_argument('-e', '--env', default="dev",
        help='environment. e.g. `dev` (default) or bee name `terra-marymorg`')
    parser.add_argument('-p', '--billing_project', required=False,
        help='billing project to create workspace in')
    parser.add_argument('-w', '--workspace_name', required=False,
        help='name of workspace to be created, if left blank will be auto-generated')
    parser.add_argument('-b', '--is-bee', action='store_true',
        help='flag that the environment is a bee')

    args = parser.parse_args()


# Setup configuration
# These values should be injected into the environment before setup
if args.token:
    azure_token = args.token
else:
    azure_token = os.environ.get("AZURE_TOKEN")

if args.env:
    if args.is_bee:
        env_string = f"{args.env}.bee.envs-terra.bio"
    else:
        env_string = f"dsde-{args.env}.broadinstitute.org"
else:
    bee_name = os.environ.get("BEE_NAME")
    env_string = bee_name + ".bee.envs-terra.bio"

if args:
    billing_project_name = args.billing_project
else:
    billing_project_name = os.environ.get("BILLING_PROJECT_NAME")

if args.workspace_name:
    workspace_name = args.workspace_name
else:
    workspace_name = ""

rawls_url = f"https://rawls.{env_string}"
leo_url = f"https://leonardo.{env_string}"

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


# Create ImputationBeagle method in CBAS
def create_cbas_method(cbas_url, workspace_id):
    method_name = "ImputationBeagle_hg38"
    request_body = {
        "method_name": method_name,
        "method_source": "GitHub",
        "method_url": "https://github.com/broadinstitute/warp/blob/TSPS-183_mma_beagle_imputation_hg38/pipelines/broad/arrays/imputation_beagle/ImputationBeagle.wdl",
        "method_version": "TSPS-183_mma_beagle_imputation_hg38"
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



# ---------------------- Start TSPS Azure E2E test ----------------------
found_exception = False
try:
    logging.info("Starting Workflows Azure E2E test...")

    # Create workspace
    logging.info("Creating workspace...")
    workspace_id, workspace_name = create_workspace(billing_project_name, azure_token, rawls_url, workspace_name)

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
    upload_wds_data_using_api(wds_url, workspace_id, "e2e-test/resources/tsps/imputation_beagle_hg38.tsv", "imputation_beagle_hg38")

    # create a new method
    method_id = create_cbas_method(cbas_url, workspace_id)
    method_version_id = get_method_version_id(cbas_url, method_id)

except Exception as e:
    logging.error(f"Exception(s) occurred during test. Details: {e}")
    found_exception = True

