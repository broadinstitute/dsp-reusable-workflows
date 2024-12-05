import logging
import requests
import json
import uuid
import time
import tempfile
import urllib.parse

# configure logging format
LOG_FORMAT = "%(asctime)s %(levelname)-8s %(message)s"
LOG_LEVEL = "INFO"
LOG_DATEFORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(
    format=LOG_FORMAT,
    level=getattr(logging, LOG_LEVEL),
    datefmt=LOG_DATEFORMAT,
)

# update workspace id for imputation beagle pipeline
def update_imputation_pipeline_workspace(teaspoons_url, workspace_project, workspace_name, wdl_method_version, token):
    request_body = {
        "workspaceBillingProject": workspace_project,
        "workspaceName": workspace_name,
        "wdlMethodVersion": wdl_method_version
    }

    uri = f"{teaspoons_url}/api/admin/v1/pipelines/array_imputation/0"
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


def prepare_imputation_pipeline(teaspoons_url, token):
    request_body = {
        "jobId": f'{uuid.uuid4()}',
        "pipelineName": "array_imputation",
        "pipelineVersion": 0,
        "pipelineInputs": {
            "multiSampleVcf": "this/is/a/fake/file.vcf.gz",
            "outputBasename": "fake_basename"
        }
    }

    uri = f"{teaspoons_url}/api/pipelineruns/v1/prepare"
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
def start_imputation_pipeline(jobId, teaspoons_url, token):
    request_body = {
        "description": f"e2e test run for jobId {jobId}",
        "jobControl": {
            "id": f'{jobId}'
        }
    }

    uri = f"{teaspoons_url}/api/pipelineruns/v1/start"
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

    return response['jobReport']['id'], response['jobReport']['resultURL']


# poll for imputation beagle job; if successful, return the pipelineRunReport.outputs object (dict)
def poll_for_imputation_job(result_url, token):

    logging.info("sleeping for 5 minutes so pipeline has time to complete")
    # start by sleeping for 5 minutes
    time.sleep(5 * 60)

    # waiting for 25 total minutes, initial 5 minutes then 20 intervals of 1 minute each
    poll_count = 20
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    while poll_count >= 0:
        response = requests.get(result_url, headers=headers)
        status_code = response.status_code

        if status_code == 200:
            # job is completed, test for status
            response = json.loads(response.text)
            logging.info(f'teaspoons pipeline completed with 200 status')
            if response['jobReport']['status'] == 'SUCCEEDED':
                logging.info(f"teaspoons pipeline has succeeded: {response}")
                # return the pipeline output dictionary
                return response['pipelineRunReport']['outputs']
            else:
                raise Exception(f'teaspoons pipeline failed: {response}')
        elif status_code == 202:
            logging.info("teaspoons pipeline still running, sleeping for 1 minute")
            # job is still running, sleep for the next poll
            time.sleep(1 * 60)
        else:
            raise Exception(f'teaspoons pipeline failed with a {status_code} status code. has response {response.text}')
        poll_count -= 1

    raise Exception(f"teaspoons pipeline did not complete in 25 minutes")

def query_for_user_quota_consumed(teaspoons_url, token):

    uri = f"{teaspoons_url}/api/quotas/v1/array_imputation"
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    response = requests.get(uri, headers=headers)
    status_code = response.status_code

    if status_code != 200:
        raise Exception(response.text)

    logging.info(f"Successfully retried user quota")
    response = json.loads(response.text)

    return response['quotaConsumed']

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
