from workspace_helper import create_workspace, delete_workspace
from app_helper import poll_for_app_url
import uuid
import random
import string
import wds_client
import requests
import logging

logging.basicConfig(level=logging.DEBUG)

workspace_manager_url = ""
rawls_url = ""
leo_url = ""

def setup(bee_name):
    # define major service endpoints based on bee name
    global workspace_manager_url
    workspace_manager_url = f"https://workspace.{bee_name}.bee.envs-terra.bio"
    logging.debug(f"workspace_manager_url: {workspace_manager_url}")
    global rawls_url
    rawls_url = f"https://rawls.{bee_name}.bee.envs-terra.bio"
    logging.debug(f"rawls_url: {rawls_url}")
    global leo_url
    leo_url = f"https://leonardo.{bee_name}.bee.envs-terra.bio"
    logging.debug(f"leo_url: {leo_url}")
    return workspace_manager_url, rawls_url, leo_url

# UPLOAD DATA TO WORKSPACE DATA SERVICE IN A WORKSPACE
def upload_wds_data(wds_url, current_workspaceId, tsv_file_name, recordName, azure_token):
    version="v0.2"
    api_client = wds_client.ApiClient(header_name='Authorization', header_value="Bearer " + azure_token)
    api_client.configuration.host = wds_url
    
    # records client is used to interact with Records in the data table
    records_client = wds_client.RecordsApi(api_client)

    # determine number of lines in table
    with open(tsv_file_name, 'r') as file:
        for count, line in enumerate(file):
            pass

    # data to upload to wds table
    logging.info("uploading to wds")
    # Upload entity to workspace data table with name "testType_uploaded"
    response = records_client.upload_tsv(current_workspaceId, version, recordName, tsv_file_name)
    logging.debug(response)
    assert response.records_modified == count, f"Uploading to wds failed: {response.reason}"
    # return true if upload succeeded
    return response.records_modified == count

def clone_workspace(billing_project_name, workspace_name, header):
    clone_workspace_api = f"{rawls_url}/api/workspaces/{billing_project_name}/{workspace_name}/clone";
    request_body = {
        "namespace": billing_project_name,  # Billing project name
        "name": f"{workspace_name} clone-{''.join(random.choices(string.ascii_lowercase, k=3))}",  # workspace name
        "attributes": {}};

    logging.info(f"cloning workspace {workspace_name}")
    response = requests.post(url=clone_workspace_api, json=request_body, headers=header)
    assert response.status_code == 201, f"Cloning {workspace_name} failed: {response.reason}"
    # example json that is returned by request: 'attributes': {}, 'authorizationDomain': [], 'bucketName': '', 'createdBy': 'yulialovesterra@gmail.com', 'createdDate': '2023-08-03T20:10:59.116Z', 'googleProject': '', 'isLocked': False, 'lastModified': '2023-08-03T20:10:59.116Z', 'name': 'api-workspace-1', 'namespace': 'yuliadub-test2', 'workspaceId': 'ac466322-2325-4f57-895d-fdd6c3f8c7ad', 'workspaceType': 'mc', 'workspaceVersion': 'v2'}
    clone_response_json = response.json()
    logging.debug(clone_response_json)
    return clone_response_json["workspaceId"], clone_response_json["name"]

def check_wds_data(wds_url, workspaceId, recordName, azure_token):
    version = "v0.2"
    api_client = wds_client.ApiClient(header_name='Authorization', header_value="Bearer " + azure_token)
    api_client.configuration.host = wds_url

    schema_client = wds_client.SchemaApi(api_client)

    logging.info("verifying data was cloned")
    response = schema_client.describe_record_type(workspaceId, version, recordName);
    assert response.name == recordName, "Name does not match"
    assert response.count == 2506, "Count does not match"

def test_cleanup(billing_project_name, workspace_name, azure_token):
    try:
        delete_workspace(billing_project_name, workspace_name, rawls_url, azure_token)
        logging.info("Workspace cleanup complete")
    # Catch the exception and continue with the test since we don't want cleanup to affect the test results.
    except Exception as e:
        logging.warning(f"Error cleaning up workspace, test script will continue. Error details: {e}")


def add_user_to_billing_profile(rawls_url, billing_project_name, email_to_share, owner_token):
    request_body = {
        "membersToAdd": [
            {"email": f"{email_to_share}", "role": "User"}
        ],
        "membersToRemove": []
    }

    uri = f"{rawls_url}/api/billing/v2/{billing_project_name}/members?inviteUsersNotFound=true"
    headers = {
        "Authorization": f"Bearer {owner_token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    response = requests.patch(uri, json=request_body, headers=headers)
    status_code = response.status_code

    if status_code != 204:
        raise Exception(response.text)

    logging.info(f"Successfully added {email_to_share} as a User to billing project {billing_project_name}")

def create_gcp_billing_project(rawls_url, billing_project_name, billing_account_name, owner_token):
    uri = f"{rawls_url}/api/billing/v2"
    request_body = {
        "projectName": billing_project_name,
        "billingAccount": billing_account_name
    }
    headers = {
        "Authorization": f"Bearer {owner_token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    response = requests.post(uri, json=request_body, headers=headers)

    if response.status_code != 201:
        raise Exception(response.text)

    logging.info(f"Successfully created GCP billing project {billing_project_name}")

def delete_gcp_billing_project(rawls_url, billing_project_name, owner_token):
    uri = f"{rawls_url}/api/billing/v2/{billing_project_name}"
    headers = {
        "Authorization": f"Bearer {owner_token}",
        "accept": "application/json"
    }

    response = requests.delete(uri, headers=headers)

    if response.status_code != 204:
        raise Exception(response.text)

    logging.info(f"Successfully deleted GCP billing project {billing_project_name}")
