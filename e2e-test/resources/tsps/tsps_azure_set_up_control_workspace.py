import argparse
import requests
import time
import logging
import sys
import os
 
# getting the name of the directory
# where this file is present.
current = os.path.dirname(os.path.realpath(__file__))
 
# Getting the parent directory name
# where the current directory is present.
grandparent = os.path.dirname(os.path.dirname(current))
 
# adding the parent directory to 
# the sys.path.
sys.path.append(grandparent)
 
# importing
from workspace_helper import create_workspace, share_workspace_grant_owner
from app_helper import create_app, poll_for_app_url
from cbas_helper import create_cbas_method


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run e2e test for TSPS/Imputation service')
    parser.add_argument('-t', '--user_token', required=False,
        help='token for user to authenticate Terra API calls')
    parser.add_argument('-m', '--tsps_sa_email', required=False,
                        help='email of tsps service account to share workspace/billing project with')
    parser.add_argument('-e', '--env', required=False, default='dev',
        help='environment. e.g. `dev` (default) or bee name `terra-marymorg`')
    parser.add_argument('-p', '--billing_project', required=False,
        help='billing project to create workspace in')
    parser.add_argument('-w', '--workspace_name', required=False,
        help='name of workspace to be created, if left blank will be auto-generated')
    parser.add_argument('-b', '--is-bee', action='store_true',
        help='flag that the environment is a bee')
    args = parser.parse_args()


# Setup configuration
azure_token = args.user_token

if args.is_bee:
    env_string = f"{args.env}.bee.envs-terra.bio"
else:
    env_string = f"dsde-{args.env}.broadinstitute.org"

billing_project_name = args.billing_project
workspace_name = args.workspace_name
tsps_sa_email = args.tsps_sa_email

rawls_url = f"https://rawls.{env_string}"
leo_url = f"https://leonardo.{env_string}"
firecloud_orch_url = f"https://firecloudorch.{env_string}"
tsps_url = f"https://tsps.{env_string}"

# configure logging format
LOG_FORMAT = "%(asctime)s %(levelname)-8s %(message)s"
LOG_LEVEL = "INFO"
LOG_DATEFORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(
    format=LOG_FORMAT,
    level=getattr(logging, LOG_LEVEL),
    datefmt=LOG_DATEFORMAT,
)


# Check whether workspace exists
def workspace_exists(billing_project_name, workspace_name, rawls_url, token):
    # returns workspace_id if workspace exists, or None if not

    rawls_workspace_api = f"{rawls_url}/api/workspaces/{billing_project_name}/{workspace_name}"
    
    header = {
        "Authorization": "Bearer " + token,
        "accept": "application/json"
    }

    response = requests.get(url=rawls_workspace_api, headers=header)

    status_code = response.status_code

    if status_code != 200:
        return None
    
    response_json = response.json()
    return response_json['workspace']['workspaceId']


# Upload data to WDS
def upload_wds_data(wds_url, workspace_id, tsv_file_name, record_name, token):
    #open TSV file in read mode
    with open(tsv_file_name) as tsv_file:
        request_file = tsv_file.read()

    uri = f"{wds_url}/{workspace_id}/tsv/v0.2/{record_name}"
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.post(uri, files={'records':request_file}, headers=headers)

    status_code = response.status_code

    if status_code != 200:
        raise Exception(response.text)

    logging.info(f"Successfully uploaded data to WDS. Response: {response.json()}")


# ---------------------- Start TSPS Imputation Workspace Setup ----------------------

logging.info("Starting TSPS imputation workspace setup...")

if workspace_name == "":
    workspace_id = None
else:
    # Check if workspace exists already
    logging.info(f"Checking if workspace {billing_project_name}/{workspace_name} exists...")
    workspace_id = workspace_exists(billing_project_name, workspace_name, rawls_url, azure_token)
    logging.info(f"Workspace {'already exists' if workspace_id else 'does not yet exist'}")

if not(workspace_id):
    # Create workspace
    logging.info("Creating workspace...")
    workspace_id, workspace_name = create_workspace(billing_project_name, azure_token, rawls_url, workspace_name)

    # sleep for 1 minute to allow apps that auto-launch to start provisioning
    logging.info("Sleeping for 1 minute to allow apps that auto-launch to start provisioning...")
    time.sleep(60)

# share created workspace with the tsps service account
if tsps_sa_email:
    logging.info("sharing workspace with tsps qa service account")
    share_workspace_grant_owner(firecloud_orch_url, billing_project_name, workspace_name,
                    tsps_sa_email, azure_token)

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
upload_wds_data(wds_url, 
                workspace_id, 
                "e2e-test/resources/tsps/imputation_beagle_hg38.tsv", "imputation_beagle_hg38",
                azure_token)

# create a new method
method_id = create_cbas_method(cbas_url, 
                               workspace_id, 
                               "ImputationBeagle",
                               "https://github.com/broadinstitute/warp/blob/TSPS-183_mma_beagle_imputation_hg38/pipelines/broad/arrays/imputation_beagle/ImputationBeagle.wdl", 
                               azure_token)
