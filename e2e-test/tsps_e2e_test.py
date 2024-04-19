from workspace_helper import create_workspace, delete_workspace
from app_helper import create_app, poll_for_app_url
import argparse
import requests
import os
import json
import uuid
import time
import logging


# Upload data to WDS using APIs
def upload_wds_data_using_api(wds_url, workspace_id, tsv_file_name, record_name):
    #open TSV file in read mode
    with open(tsv_file_name) as tsv_file:
        request_file = tsv_file.read()

    uri = f"{wds_url}/{workspace_id}/tsv/v0.2/{record_name}"
    headers = {"Authorization": f"Bearer {azure_user_token}"}

    response = requests.post(uri, files={'records':request_file}, headers=headers)

    status_code = response.status_code

    if status_code != 200:
        raise Exception(response.text)

    logging.info(f"Successfully uploaded data to WDS. Response: {response.json()}")


# Create imputation method in CBAS
def create_imputation_method(cbas_url, workspace_id, token):
    method_name = "ImputationBeagle"
    request_body = {
        "method_name": method_name,
        "method_source": "GitHub",
        # "method_url": "https://github.com/broadinstitute/warp/blob/TSPS-183_mma_beagle_imputation_hg38/pipelines/broad/arrays/imputation_beagle/ImputationBeagle.wdl",
        "method_url": "https://github.com/broadinstitute/warp/blob/js_try_imputation_azure/pipelines/broad/arrays/imputation/hello_world_no_file_input.wdl",
        "method_version": "1"
    }

    uri = f"{cbas_url}/api/batch/v1/methods"
    headers = {
        "Authorization": f"Bearer {token}",
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


def share_workspace(orch_url, billing_project_name, workspace_name, email_to_share, owner_token):
    request_body = [{
        "email": f"{email_to_share}",
        "accessLevel": "OWNER",
        "canShare": True,
        "canCompute": True
    }]

    uri = f"{orch_url}/api/workspaces/{billing_project_name}/{workspace_name}/acl"
    headers = {
        "Authorization": f"Bearer {owner_token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    response = requests.patch(uri, json=request_body, headers=headers)
    status_code = response.status_code

    if status_code != 200:
        raise Exception(response.text)

    logging.info(f"Successfully shared workspace {workspace_name} with {email_to_share}")


# run imputation pipeline
def run_imputation_pipeline(tsps_url, token):
    request_body = {
        "description": "string",
        "jobControl": {
            "id": f'{uuid.uuid4()}'
        },
        "pipelineVersion": "string",
        "pipelineInputs": {}
    }

    uri = f"{tsps_url}/api/pipelines/v1/imputation_beagle"
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    response = requests.post(uri, json=request_body, headers=headers)
    status_code = response.status_code

    if status_code != 202:
        raise Exception(response.text)

    logging.info(f"Successfully launched imputation pipeline")
    response = json.loads(response.text)

    return response['jobReport']['id']


# run imputation pipeline
def poll_for_imputation_job(tsps_url, job_id, token):

    # start by sleeping for 5 minutes
    time.sleep(5 * 60)

    # waiting for 25 total minutes, initial 5 minutes then 20 intervals of 1 minute each
    poll_count = 20
    uri = f"{tsps_url}/api/pipelines/job/v1/imputation_beagle/result/{job_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    while poll_count >= 0:
        response = requests.get(uri, headers=headers)
        status_code = response.status_code

        if status_code == 200:
            # job is completed, test for status
            response = json.loads(response.text)
            if response['status'] != 'SUCCEEDED':
                raise Exception(f'tsps pipeline failed: {response}')
        elif status_code == 202:
            logging.info("tsps pipeline still running, sleeping for 1 minute")
            # job is still running, sleep for the next poll
            time.sleep(1 * 60)
        else:
            raise Exception(response.text)
        poll_count -= 1

    raise Exception(f"tsps pipeline did not complete in 25 minutes")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run e2e test for TSPS/Imputation service')
    parser.add_argument('-t', '--user_token', required=False,
        help='token for user to authenticate Terra API calls')
    parser.add_argument('-s', '--tsps_sa_token', required=False,
                        help='token for tsps SA to authenticate Terra API calls')
    parser.add_argument('-e', '--env', required=False,
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
if args.user_token:
    azure_user_token = args.user_token
else:
    azure_user_token = os.environ.get("AZURE_USER_TOKEN")

if args.tsps_sa_token:
    azure_tsps_sa_token = args.tsps_sa_token
else:
    azure_tsps_sa_token = os.environ.get("AZURE_TSPS_SA_TOKEN")

if args.env:
    if args.is_bee:
        env_string = f"{args.env}.bee.envs-terra.bio"
    else:
        env_string = f"dsde-{args.env}.broadinstitute.org"
else:
    bee_name = os.environ.get("BEE_NAME")
    env_string = bee_name + ".bee.envs-terra.bio"

if args.billing_project:
    billing_project_name = args.billing_project
else:
    billing_project_name = os.environ.get("BILLING_PROJECT_NAME")

if args.workspace_name:
    workspace_name = args.workspace_name
else:
    workspace_name = ""

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

# ---------------------- Start TSPS Azure E2E test ----------------------
found_exception = False
try:
    logging.info("Starting Workflows Azure E2E test...")
    logging.info(f"billing project: {billing_project_name}, env_string: {env_string}")

    # Create workspace
    logging.info("Creating workspace...")
    workspace_id, workspace_name = create_workspace(billing_project_name, azure_user_token, rawls_url, workspace_name)

    # sleep for 1 minute to allow apps that auto-launch to start provisioning
    logging.info("Sleeping for 1 minute to allow apps that auto-launch to start provisioning...")
    time.sleep(60)

    # share created workspace with the tsps service account
    logging.info("sharing workspace with tsps qa service account")
    share_workspace(firecloud_orch_url, billing_project_name, workspace_name,
                    "tsps-qa@broad-dsde-qa.iam.gserviceaccount.com", azure_user_token)

    share_workspace(firecloud_orch_url, billing_project_name, workspace_name,
                    "jsoto@test.firecloud.org", azure_user_token)

    # After "Multi-user Workflow: Auto app start up" phase is completed, WORKFLOWS_APP will be launched
    # automatically at workspace creation time (similar to WDS). So to prevent test failures and errors
    # (until script is updated) when the code for that phase is released in dev, we check here if the WORKFLOWS_APP
    # is already deployed before manually creating it. `poll_for_app_url` before starting to poll checks & returns
    # "" if the app was never deployed
    # Note: After "Multi-user Workflow: Auto app start up" phase is completed, update the script and remove
    #       these 4 lines as we already poll for CBAS proxy url down below
    logging.info("Checking to see if WORKFLOWS_APP was deployed...")
    cbas_url = poll_for_app_url(workspace_id, 'WORKFLOWS_APP', 'cbas', azure_user_token, leo_url)
    if cbas_url == "":
        create_app(workspace_id, leo_url, 'WORKFLOWS_APP', 'WORKSPACE_SHARED', azure_user_token)

    # Since CROMWELL_RUNNER app needs the `cromwellmetadata` database available before it can be deployed,
    # wait for WORKFLOWS app to be in Running state before deploying CROMWELL_RUNNER app.
    # Check that CBAS is ready; if not fail the test after 10 minutes of polling
    logging.info(f"Polling to check if WORKFLOWS app is ready in workspace {workspace_id}...")
    cbas_url = poll_for_app_url(workspace_id, 'WORKFLOWS_APP', 'cbas', azure_user_token, leo_url)
    if cbas_url == "":
        raise Exception(f"WORKFLOWS app not ready or errored out for workspace {workspace_id}")

    # Create CROMWELL_RUNNER app in workspace
    logging.info("creating cromwell runner app")
    create_app(workspace_id, leo_url, 'CROMWELL_RUNNER_APP', 'USER_PRIVATE', azure_tsps_sa_token)

    # check that Cromwell Runner is ready; if not fail the test after 10 minutes of polling
    logging.info(f"Polling to check if CROMWELL_RUNNER app is ready in workspace {workspace_id}...")
    cromwell_url = poll_for_app_url(workspace_id, 'CROMWELL_RUNNER_APP', 'cromwell-runner', azure_tsps_sa_token, leo_url)
    if cromwell_url == "":
        raise Exception(f"CROMWELL_RUNNER app not ready or errored out for workspace {workspace_id}")

    # check that WDS is ready; if not fail the test after 10 minutes of polling
    logging.info(f"Polling to check if WDS app is ready to upload data for workspace {workspace_id}...")
    wds_url = poll_for_app_url(workspace_id, 'WDS', 'wds', azure_user_token, leo_url)
    if wds_url == "":
        raise Exception(f"WDS app not ready or errored out for workspace {workspace_id}")

    # # upload data to workspace
    # upload_wds_data_using_api(wds_url, workspace_id, "e2e-test/resources/tsps/imputation_beagle_hg38.tsv", "imputation_beagle_hg38")

    # create a new imputation method that tsps will run
    logging.info("creating imputation method")
    method_id = create_imputation_method(cbas_url, workspace_id, azure_user_token)

    # use admin endpoint to set imputation workspace id

    # launch tsps imputation pipeline
    logging.info("running imputation pipeline")
    job_id = run_imputation_pipeline(tsps_url, azure_user_token)

    # poll for imputation pipeline
    logging.info("polling for imputation pipeline")
    poll_for_imputation_job(tsps_url, job_id, azure_user_token)


except Exception as e:
    logging.error(f"Exception(s) occurred during test. Details: {e}")
    found_exception = True
finally:
    # # delete workspace and apps
    # logging.info("Starting workspace cleanup...")
    # test_cleanup(workspace_name)

    # Use exit(1) so that GHA will fail if an exception was found during the test
    if found_exception:
        logging.error("Exceptions found during test run. Test failed")
        exit(1)
    else:
        logging.info("Test completed successfully")
