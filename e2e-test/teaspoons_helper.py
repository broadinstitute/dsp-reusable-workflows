import logging
import requests
import json
import os
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
    force=True  # override any existing logging configuration
)

# update workspace id for imputation beagle pipeline
def update_imputation_pipeline_workspace(teaspoons_url, workspace_project, workspace_name, wdl_method_version, token):
    request_body = {
        "workspaceBillingProject": workspace_project,
        "workspaceName": workspace_name,
        "toolVersion": wdl_method_version
    }

    uri = f"{teaspoons_url}/api/admin/v1/pipelines/array_imputation/1"
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    response = requests.patch(uri, json=request_body, headers=headers)
    status_code = response.status_code

    if status_code != 200:
        raise Exception(response.text)

    logging.info(f"successfully updated imputation pipeline workspace and tool version to: {workspace_project}, {workspace_name}, {wdl_method_version}")


def prepare_imputation_pipeline(teaspoons_url, multi_sample_vcf_input_path, output_basename, token):
    request_body = {
        "jobId": f'{uuid.uuid4()}',
        "pipelineName": "array_imputation",
        "pipelineVersion": 1,
        "pipelineInputs": {
            "multiSampleVcf": f'{multi_sample_vcf_input_path}',
            "outputBasename": f'{output_basename}'
        },
        "description": "GHA e2e test run"
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

    logging.info(f"Successfully prepared imputation pipeline run. Job ID: {request_body['jobId']}")
    response = json.loads(response.text)

    return response['jobId'], response['fileInputUploadUrls']

# run imputation beagle pipeline
def start_imputation_pipeline(jobId, teaspoons_url, token):
    request_body = {
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
def poll_for_imputation_job(result_url, sleep_interval_mins=1, total_timeout_mins=25, token=None, credentials=None):
    # start by sleeping for 5 minutes
    logging.info("Sleeping for 5 minutes before polling for status...")
    time.sleep(5 * 60)

    remaining_timeout_minutes = total_timeout_mins - 5
    poll_count = remaining_timeout_minutes / sleep_interval_mins

    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    # poll for job completion until we get a 200 status code, or we hit our timeout
    while poll_count >= 0:
        response = requests.get(result_url, headers=headers)
        status_code = response.status_code

        if status_code == 200:
            # job is completed, test for status
            response = json.loads(response.text)
            logging.info(f'Pipeline run completed with 200 status')
            if response['jobReport']['status'] == 'SUCCEEDED':
                # return the pipeline output dictionary
                return response['pipelineRunReport']['outputs']
            else:
                raise Exception(f'Pipeline run failed. Response: {response}')
        elif status_code == 202:
            logging.info(f"Pipeline is still Running. Sleeping for {sleep_interval_mins} minute(s) before polling again")
            # job is still running, sleep for the next poll
            time.sleep(sleep_interval_mins * 60)
        else:
            raise Exception(f'Pipeline run failed with a {status_code} status code. Response: {response.text}')
        poll_count -= 1

    raise Exception(f"Pipeline run did not complete in {total_timeout_mins} minutes. Timing out.")

def get_output_signed_urls(teaspoons_url, job_id, token):
    """
	    Retrieve signed URLs for the outputs of a completed pipeline run.
	
	    This function calls the v2 teaspoons pipeline runs API to obtain
	    time-limited signed URLs for the output artifacts associated with
	    the specified job ID.
	
	    :param teaspoons_url: Base URL of the teaspoons service (e.g. https://host).
	    :param job_id: Identifier of the pipeline job whose outputs are requested.
	    :param token: User's bearer token used for authorization with the teaspoons API.
	    :return: The ``outputSignedUrls`` field from the API response, typically a
	             mapping of output names to their corresponding signed URLs.
	    :raises Exception: If the HTTP response status code is not 200 or the API
	                       returns an error payload.
	    """

    uri = f"{teaspoons_url}/api/pipelineruns/v2/result/{job_id}/output/signed-urls"
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }
    response = requests.get(uri, headers=headers)
    status_code = response.status_code

    if status_code != 200:
        raise Exception(response.text)
    
    response = json.loads(response.text)

    logging.info(f"Successfully retrieved output signed urls")

    return response['outputSignedUrls']

# Get quota details for a user, including quota limit and quota consumed for the array imputation pipeline
def get_user_quota_details(teaspoons_url, token):

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
    
    response = json.loads(response.text)

    logging.info(f"Retrieved quota details for the test user")

    return response

## GCS FILE FUNCTIONS
# Upload a file to a signed url
def upload_file_with_signed_url(signed_url, local_file_path):
    with open(file=local_file_path, mode="rb") as blob_file:
        data = blob_file.read()
        logging.info("Preparing to upload file to signed url")
        headers = {
            'Content-Type': 'application/octet-stream'
        }
        requests.put(signed_url, headers=headers, data=data)

# write a hello world text file and upload using a signed url
def upload_mock_file_with_signed_url(signed_url):
    # use a temporary directory that will get cleaned up after this block
    with tempfile.TemporaryDirectory() as tmpdirname:
        local_file_path = os.path.join(tmpdirname, "temp_file")

        # write the file locally
        with open(file=local_file_path, mode="w") as blob_file:
            blob_file.write("Hello, World!")
        
        # upload the file
        upload_file_with_signed_url(signed_url, local_file_path)

# download a file from a signed url
def validate_output_not_empty(output_name, signed_url):
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

        file_size = os.path.getsize(local_file_path)
        if file_size == 0:
            raise Exception(f"Output file for `{output_name}` is empty")

        logging.info(f"Successfully downloaded output `{output_name}`. File size: {file_size} bytes")

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


# sometimes we want to check if a service is pingable before running our e2e test due to some transient
# issues we've been seeing.
def ping_until_200_with_timeout(url, timeout_seconds=300):
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return
        except requests.exceptions.ConnectionError:
            logging.warning(f"URL {url} not reachable")
        time.sleep(30)
    raise Exception(f"Timed out waiting for {url} to return 200")

# update the quota limit for a user for the array_imputation_pipeline
def update_quota_limit_for_user(sam_url, teaspoons_url, admin_token, user_email, new_quota_limit):

    uri = f"{sam_url}/api/admin/v1/user/email/{user_email}"
    headers = {
        "Authorization": f"Bearer {admin_token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    response = requests.get(uri, headers=headers)
    status_code = response.status_code

    if status_code != 200:
        raise Exception(response.text)

    logging.info(f"Retrieved user info for {user_email} from Sam")

    # parse the response to get the user ID
    response = json.loads(response.text)
    user_id = response['userInfo']['userSubjectId']

    uri = f"{teaspoons_url}/api/admin/v1/quotas/array_imputation/{user_id}"
    headers = {
        "Authorization": f"Bearer {admin_token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    request_body = {
        "quotaLimit": new_quota_limit
    }

    response = requests.patch(uri, headers=headers, json=request_body)
    status_code = response.status_code
    if status_code != 200:
        raise Exception(response.text)

    logging.info(f"Successfully updated quota for user {user_email} to {new_quota_limit}")
