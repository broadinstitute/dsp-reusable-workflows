from workspace_helper import create_gcp_workspace, delete_workspace, share_workspace_grant_owner, add_wdl_to_gcp_workspace
from helper import create_gcp_billing_project, delete_gcp_billing_project

import requests
import os
import json
import uuid
import time
import logging
import tempfile
import urllib.parse


# update workspace id for imputation beagle pipeline
def update_imputation_pipeline_workspace(tsps_url, workspace_project, workspace_name, wdl_method_version, token):
    request_body = {
        "workspaceBillingProject": workspace_project,
        "workspaceName": workspace_name,
        "wdlMethodVersion": wdl_method_version
    }

    uri = f"{tsps_url}/api/admin/v1/pipeline/array_imputation"
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    response = requests.patch(uri, json=request_body, headers=headers)
    status_code = response.status_code

    if status_code != 200:
        raise Exception(response.text)

    logging.info(f"successfully updated imputation pipeline workspace and wdl method version to: {workspace_project}, {workspace_name}, {wdl_method_version}")


def prepare_imputation_pipeline(tsps_url, token):
    request_body = {
        "jobId": f'{uuid.uuid4()}',
        "pipelineVersion": 0,
        "pipelineInputs": {
            "multiSampleVcf": "this/is/a/fake/file.vcf.gz",
            "outputBasename": "fake_basename"
        }
    }

    uri = f"{tsps_url}/api/pipelineruns/v1/array_imputation/prepare"
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    response = requests.post(uri, json=request_body, headers=headers)
    status_code = response.status_code

    if status_code != 200:
        raise Exception(response.text)

    logging.info(f"Successfully prepared imputation pipeline run")
    response = json.loads(response.text)

    return response['jobId'], response['fileInputUploadUrls']

# run imputation beagle pipeline
def start_imputation_pipeline(jobId, tsps_url, token):
    request_body = {
        "description": f"e2e test run for jobId {jobId}",
        "jobControl": {
            "id": f'{jobId}'
        }
    }

    uri = f"{tsps_url}/api/pipelineruns/v1/array_imputation/start"
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    response = requests.post(uri, json=request_body, headers=headers)
    status_code = response.status_code

    if status_code != 202:
        raise Exception(response.text)

    logging.info(f"Successfully started imputation pipeline run")
    response = json.loads(response.text)

    return response['jobReport']['id']


# poll for imputation beagle job; if successful, return the pipelineRunReport.outputs object (dict)
def poll_for_imputation_job(tsps_url, job_id, token):

    logging.info("sleeping for 5 minutes so pipeline has time to complete")
    # start by sleeping for 5 minutes
    time.sleep(5 * 60)

    # waiting for 25 total minutes, initial 5 minutes then 20 intervals of 1 minute each
    poll_count = 20
    uri = f"{tsps_url}/api/pipelineruns/v1/array_imputation/result/{job_id}"
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
                return response['pipelineRunReport']['outputs']
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


## GCS FILE FUNCTIONS
# write a hello world text file and upload using a signed url
def upload_file_with_signed_url(signed_url):
    # use a temporary directory that will get cleaned up after this block
    with tempfile.TemporaryDirectory() as tmpdirname:
        local_file_path = os.path.join(tmpdirname, "temp_file")

        # write the file locally
        with open(file=local_file_path, mode="w") as blob_file:
            blob_file.write("Hello, World!")
        
        # upload the file
        with open(file=local_file_path, mode="rb") as blob_file:
            data = blob_file.read()
            logging.info("preparing to upload blob")
            headers = {
                'Content-Type': 'application/octet-stream'
            }
            requests.put(signed_url, headers=headers, data=data)

# download a file from a signed url
def download_with_signed_url(signed_url):
    # extract file name from signed url; signed url looks like:
    # https://storage.googleapis.com/fc-secure-6970c3a9-dc92-436d-af3d-917bcb4cf05a/test_signed_urls/helloworld.txt?x-goog-signature...
    local_file_name = signed_url.split("?")[0].split("/")[-1]
    # use a temporary directory that will get cleaned up after this block
    with tempfile.TemporaryDirectory() as tmpdirname:
        local_file_path = os.path.join(tmpdirname, local_file_name)
        
        # download the file and write to local file
        with open(file=local_file_path, mode="wb") as blob_file:
            download_stream = requests.get(signed_url).content
            blob_file.write(download_stream)

## GROUP MANAGEMENT FUNCTIONS
def create_and_populate_terra_group(orch_url, group_name, group_admins, group_members, token):
    # first create the group
    group_email = create_terra_group(orch_url, group_name, token)

    # now add admins and members
    for email_address in group_admins:
        add_member_to_terra_group(orch_url, group_name, email_address, "admin", token)

    for email_address in group_members:
        add_member_to_terra_group(orch_url, group_name, email_address, "member", token)
    
    return group_email


def create_terra_group(orch_url, group_name, token):
    uri = f"{orch_url}/api/groups/{group_name}"
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json"
    }
    response = requests.post(uri, headers=headers)
    
    if response.status_code != 201:
        raise Exception(response.text)
    
    return response.json()['groupEmail']


def add_member_to_terra_group(orch_url, group_name, email_address, role, token):
    formatted_email = urllib.parse.quote_plus(email_address)
    uri = f"{orch_url}/api/groups/{group_name}/{role}/{formatted_email}"
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json"
    }
    response = requests.put(uri, headers=headers)

    if response.status_code != 204:
        raise Exception(response.text)
    
    logging.info(f"added {email_address} as {role} to Terra group {group_name}")


# Setup configuration
# The environment variables are injected as part of the e2e test setup which does not pass in any args
admin_token = os.environ.get("ADMIN_TOKEN") # admin user who has access to the terra billing project
user_token = os.environ.get("USER_TOKEN") # the user who will kick off the teaspoons job
 
# e2e test is using the tsps qa service account
tsps_sa_email = "tsps-qa@broad-dsde-qa.iam.gserviceaccount.com"

bee_name = os.environ.get("BEE_NAME")
env_string = bee_name + ".bee.envs-terra.bio"

billing_account_name = os.environ.get("BILLING_ACCOUNT_NAME")
billing_project_name = os.environ.get("BILLING_PROJECT_NAME")
workspace_name = ""

rawls_url = f"https://rawls.{env_string}"
firecloud_orch_url = f"https://firecloudorch.{env_string}"
tsps_url = f"https://tsps.{env_string}"

wdl_method_version = os.environ.get("WDL_METHOD_VERSION")

# configure logging format
LOG_FORMAT = "%(asctime)s %(levelname)-8s %(message)s"
LOG_LEVEL = "INFO"
LOG_DATEFORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(
    format=LOG_FORMAT,
    level=getattr(logging, LOG_LEVEL),
    datefmt=LOG_DATEFORMAT,
)

# ---------------------- Start Teaspoons GCP E2E test ----------------------
found_exception = False
try:
    logging.info("Starting Teaspoons GCP E2E test...")
    logging.info(f"billing project: {billing_project_name}, env_string: {env_string}")

    # Create Terra billing project
    logging.info("Creating billing project...")
    create_gcp_billing_project(rawls_url, billing_project_name, billing_account_name, admin_token)

    # Create auth domain group and add Teaspoons SA as an admin
    logging.info("Creating auth domain...")
    auth_domain_name = "teaspoons-imputation-e2e-test"
    group_admins = [tsps_sa_email]
    create_and_populate_terra_group(firecloud_orch_url, auth_domain_name, group_admins, [], admin_token)

    # Create workspace
    logging.info("Creating workspace...")
    workspace_name = create_gcp_workspace(
        billing_project_name, 
        admin_token, 
        rawls_url, 
        workspace_name, 
        auth_domains=[auth_domain_name], 
        enhanced_bucket_logging=True)

    # share created workspace with the teaspoons service account
    logging.info("sharing workspace with tsps qa service account")
    share_workspace_grant_owner(firecloud_orch_url, billing_project_name, workspace_name,
                                tsps_sa_email, admin_token)

    # create a new imputation method that teaspoons will run
    logging.info("creating imputation method")
    wdl_namespace = billing_project_name
    wdl_name = "ImputationBeagle"
    root_entity_type = "array_imputation"
    method_definition_dict = {
        "methodUri": f"dockstore://github.com%2FDataBiosphere%2Fterra-scientific-pipelines-service%2FImputationBeagleEmpty/{wdl_method_version}",
        "sourceRepo": "dockstore",
        "methodPath": "github.com/DataBiosphere/terra-scientific-pipelines-service/ImputationBeagleEmpty",
        "methodVersion": wdl_method_version
    }
    add_wdl_to_gcp_workspace(billing_project_name, workspace_name, wdl_namespace, wdl_name, method_definition_dict, root_entity_type, {}, {}, firecloud_orch_url, admin_token)

    # use admin endpoint to set imputation workspace info
    logging.info("updating imputation workspace info")
    update_imputation_pipeline_workspace(tsps_url, billing_project_name, workspace_name, wdl_method_version, admin_token)

    # prepare tsps imputation pipeline run
    logging.info("preparing imputation pipeline run")
    job_id, pipeline_file_inputs = prepare_imputation_pipeline(tsps_url, user_token)

    # make sure we got a writable signed url
    for key, value in pipeline_file_inputs.items():
        logging.info(f"attempting to upload a file to {key} input")
        upload_file_with_signed_url(value['signedUrl'])
        logging.info("successfully uploaded file")

    # start pipeline run
    logging.info("starting imputation pipeline run")
    job_id_from_start_run = start_imputation_pipeline(job_id, tsps_url, user_token)

    assert(job_id == job_id_from_start_run)

    # poll for imputation pipeline
    logging.info("polling for imputation pipeline")
    pipeline_output = poll_for_imputation_job(tsps_url, job_id, user_token)

    # grab data using signed url
    for key, value in pipeline_output.items():
        logging.info(f"attempting to retrieve {key} output")
        download_with_signed_url(value)
        logging.info("successfully downloaded file")

    logging.info("TEST COMPLETE")

except Exception as e:
    logging.error(f"Exception(s) occurred during test. Details: {e}")
    found_exception = True
finally:
    # delete workspace
    logging.info("Starting workspace cleanup as part of e2e test...")
    try:
        delete_workspace(billing_project_name, workspace_name, rawls_url, admin_token)
        logging.info("Workspace cleanup complete")
    # Catch the exception and continue with the test since we don't want cleanup to affect the test results.
    # We can assume that Janitor will clean up the workspace if the test fails
    # TODO: Instead of catching exception and continuing with test, the script should fail the test once
    #       https://broadworkbench.atlassian.net/browse/WOR-1309 is fixed
    except Exception as e:
        logging.warning(f"Error cleaning up workspace, test script will continue. Error details: {e}")

    # delete billing project
    logging.info("Starting billing project cleanup as part of e2e test...")
    try:
        delete_gcp_billing_project(rawls_url, billing_project_name, admin_token)
        logging.info("Billing project cleanup complete")
    # Catch the exception and continue with the test since we don't want cleanup to affect the test results.
    # We can assume that Janitor will clean up the billing project if the test fails
    # TODO: Instead of catching exception and continuing with test, the script should fail the test once
    #       https://broadworkbench.atlassian.net/browse/WOR-1309 is fixed
    except Exception as e:
        logging.warning(f"Error cleaning up billing project, test script will continue. Error details: {e}")

    # Use exit(1) so that GHA will fail if an exception was found during the test
    if found_exception:
        logging.error("Exceptions found during test run. Test failed")
        exit(1)
    else:
        logging.info("Test completed successfully")
