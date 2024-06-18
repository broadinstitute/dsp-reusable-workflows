from workspace_helper import create_workspace, delete_workspace, share_workspace_grant_owner
from app_helper import create_app, poll_for_app_url
from cbas_helper import create_cbas_github_method
from helper import add_user_to_billing_profile

from azure.storage.blob import BlobClient

import requests
import os
import json
import uuid
import time
import logging


# update workspace id for imputation beagle pipeline
def update_imputation_pipeline_workspace_id(tsps_url, workspace_id, token):
    request_body = {
        "workspaceId": f"{workspace_id}"
    }

    uri = f"{tsps_url}/api/admin/v1/pipeline/imputation_beagle"
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    response = requests.patch(uri, json=request_body, headers=headers)
    status_code = response.status_code

    if status_code != 200:
        raise Exception(response.text)

    logging.info(f"successfully updated imputation pipeline workspace id: {workspace_id}")


# run imputation beagle pipeline
def run_imputation_pipeline(tsps_url, token):
    request_body = {
        "description": "string",
        "jobControl": {
            "id": f'{uuid.uuid4()}'
        },
        "pipelineVersion": "string",
        "pipelineInputs": {
            "multi_sample_vcf": "this/is/a/fake/file.vcf.gz",
            "output_basename": "fake_basename"
        }
    }

    uri = f"{tsps_url}/api/pipelineruns/v1/imputation_beagle"
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


# poll for imputation beagle job; if successful, return the pipelineOutput object (dict)
def poll_for_imputation_job(tsps_url, job_id, token):

    logging.info("sleeping for 5 minutes so pipeline has time to complete")
    # start by sleeping for 5 minutes
    time.sleep(5 * 60)

    # waiting for 25 total minutes, initial 5 minutes then 20 intervals of 1 minute each
    poll_count = 20
    uri = f"{tsps_url}/api/pipelineruns/v1/imputation_beagle/result/{job_id}"
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
            logging.info(f'tsps pipeline completed with 200 status')
            if response['jobReport']['status'] == 'SUCCEEDED':
                logging.info(f"tsps pipeline has succeeded: {response}")
                # return the pipeline output dictionary
                return response['pipelineOutput']
            else:
                raise Exception(f'tsps pipeline failed: {response}')
        elif status_code == 202:
            logging.info("tsps pipeline still running, sleeping for 1 minute")
            # job is still running, sleep for the next poll
            time.sleep(1 * 60)
        else:
            raise Exception(f'tsps pipeline failed with a {status_code} status code. has response {response.text}')
        poll_count -= 1

    raise Exception(f"tsps pipeline did not complete in 25 minutes")

# download a file with azcopy
def download_with_azcopy(sas_url):
    blob_client = BlobClient.from_blob_url(sas_url)
    local_file = blob_client.blob_name.split('/')[-1] # get the file name without directories
    with open(file=local_file, mode="wb") as blob_file:
        download_stream = blob_client.download_blob()
        blob_file.write(download_stream.readall())

# Setup configuration
# The environment variables are injected as part of the e2e test setup which does not pass in any args
azure_user_token = os.environ.get("AZURE_USER_TOKEN")
azure_tsps_sa_token = os.environ.get("AZURE_TSPS_SA_TOKEN")
azure_admin_token = os.environ.get("AZURE_ADMIN_TOKEN")
 
# e2e test is using the tsps qa service account
tsps_sa_email = "tsps-qa@broad-dsde-qa.iam.gserviceaccount.com"

bee_name = os.environ.get("BEE_NAME")
env_string = bee_name + ".bee.envs-terra.bio"

billing_project_name = os.environ.get("BILLING_PROJECT_NAME")
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

    # add tsps service account to billing project, this can be removed once
    # https://broadworkbench.atlassian.net/browse/WOR-1620 is addressed
    logging.info(f"adding tsps qa service account to billing project {billing_project_name}")
    add_user_to_billing_profile(rawls_url, billing_project_name, tsps_sa_email, azure_user_token)

    # share created workspace with the tsps service account
    logging.info("sharing workspace with tsps qa service account")
    share_workspace_grant_owner(firecloud_orch_url, billing_project_name, workspace_name,
                                tsps_sa_email, azure_user_token)

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

    # Create CROMWELL_RUNNER app in workspace for the TSPS SA
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

    # use admin endpoint to set imputation workspace id
    logging.info("updating imputation workspace id")
    update_imputation_pipeline_workspace_id(tsps_url, workspace_id, azure_admin_token)

    # create a new imputation method that tsps will run
    logging.info("creating imputation method")
    method_id = create_cbas_github_method(cbas_url,
                                          workspace_id,
                                    "ImputationBeagle",
                                    "https://github.com/DataBiosphere/terra-scientific-pipelines-service/blob/main/pipelines/testing/ImputationBeagleEmpty.wdl",
                                          azure_user_token)

    # launch tsps imputation pipeline
    logging.info("running imputation pipeline")
    job_id = run_imputation_pipeline(tsps_url, azure_user_token)

    # poll for imputation pipeline
    logging.info("polling for imputation pipeline")
    pipeline_output = poll_for_imputation_job(tsps_url, job_id, azure_user_token)

    # grab data using azcopy
    for key, value in pipeline_output.items():
        logging.info(f"attempting to retrieve {key} output")
        download_with_azcopy(value)


except Exception as e:
    logging.error(f"Exception(s) occurred during test. Details: {e}")
    found_exception = True
finally:
    # delete workspace and apps
    logging.info("Starting workspace cleanup as part of e2e test...")
    try:
        delete_workspace(billing_project_name, workspace_name, rawls_url, azure_user_token)
        logging.info("Workspace cleanup complete")
    # Catch the exception and continue with the test since we don't want cleanup to affect the test results.
    # We can assume that Janitor will clean up the workspace if the test fails
    # TODO: Instead of catching exception and continuing with test, the script should fail the test once
    #       https://broadworkbench.atlassian.net/browse/WOR-1309 is fixed
    except Exception as e:
        logging.warning(f"Error cleaning up workspace, test script will continue. Error details: {e}")

    # Use exit(1) so that GHA will fail if an exception was found during the test
    if found_exception:
        logging.error("Exceptions found during test run. Test failed")
        exit(1)
    else:
        logging.info("Test completed successfully")
