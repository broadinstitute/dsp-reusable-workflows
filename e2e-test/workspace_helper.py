import requests
import json
import random
import string
import time
import logging
import shared_variables


# CREATE AZURE WORKSPACE ACTION 
def create_workspace(billing_project_name, azure_token, rawls_url, workspace_name = ""):
    for i in range(0,shared_variables.RETRIES):
        try:
            # create a new workspace, need to have attributes or api call doesnt work
            rawls_workspace_api = f"{rawls_url}/api/workspaces"
            workspace_name = workspace_name if workspace_name else f"e2e-test-api-workspace-{''.join(random.choices(string.ascii_lowercase, k=5))}"
            logging.info(f"Creating workspace {workspace_name} in {billing_project_name}")
            # note that if the billing project is a protected one, the workspace will automatically be protected as well and does not need to be specified as such here
            request_body = {
                "namespace": billing_project_name,
                "name": workspace_name,
                "attributes": {}
            }
            
            header = {
                "Authorization": "Bearer " + azure_token,
                "accept": "application/json"
            }

            workspace_response = requests.post(url=rawls_workspace_api, json=request_body, headers=header)
            if workspace_response.status_code != 201:
                logging.info(f"ERROR workspace creation for '{workspace_name}' in billing project '{billing_project_name}'. Response: {workspace_response}")
                raise Exception(f"Error creating workspace: {workspace_response.text}")

            # example json that is returned by request:
            # {
            #   "attributes": {},
            #   "authorizationDomain": [],
            #   "bucketName": "",
            #   "createdBy": "yulialovesterra@gmail.com",
            #   "createdDate": "2023-08-03T20:10:59.116Z",
            #   "googleProject": "",
            #   "isLocked": False,
            #   "lastModified": "2023-08-03T20:10:59.116Z",
            #   "name": "api-workspace-1",
            #   "namespace": "yuliadub-test2",
            #   "workspaceId": "ac466322-2325-4f57-895d-fdd6c3f8c7ad",
            #   "workspaceType": "mc",
            #   "workspaceVersion": "v2"
            # }
            workspace_response_json = workspace_response.json()
            data = json.loads(json.dumps(workspace_response_json))

            workspace_id = data['workspaceId']
            logging.info(f"Successfully started workspace creation for '{workspace_name}' in billing project '{billing_project_name}'. Workspace ID returned: {workspace_id}")

            return workspace_id, data['name']

        except Exception as e:
            logging.info(f"ERROR workspace creation for '{workspace_name}' in billing project '{billing_project_name}'. Error: {e}")
            continue
    else:
        raise Exception(f"Error creating workspace: retries maxed out.")


# CREATE GCP WORKSPACE ACTION
def create_gcp_workspace(billing_project_name, token, rawls_url, workspace_name = "", auth_domains = [], enhanced_bucket_logging = False):
    for i in range(0,shared_variables.RETRIES):
        try:
            # create a new workspace, need to have attributes or api call doesnt work
            rawls_workspace_api = f"{rawls_url}/api/workspaces"
            workspace_name = workspace_name if workspace_name else f"e2e-test-api-workspace-{''.join(random.choices(string.ascii_lowercase, k=5))}"
            logging.info(f"Creating workspace {workspace_name} in {billing_project_name}")
            
            request_body = {
                "namespace": billing_project_name,
                "name": workspace_name,
                "authorizationDomain": [{"membersGroupName": auth_domain} for auth_domain in auth_domains],
                "enhancedBucketLogging": enhanced_bucket_logging,
                "attributes": {}
            }
            header = {
                "Authorization": "Bearer " + token,
                "accept": "application/json"
            }

            workspace_response = requests.post(url=rawls_workspace_api, json=request_body, headers=header)
            if workspace_response.status_code != 201:
                logging.info(f"ERROR workspace creation for '{workspace_name}' in billing project '{billing_project_name}'. Response: {workspace_response}")
                raise Exception(f"Error creating workspace: {workspace_response.text}")

            # example json that is returned by request:
            # {
            # "attributes": {},
            # "authorizationDomain": [
            #     {
            #     "membersGroupName": "example-auth-domain-group"
            #     }
            # ],
            # "billingAccount": "billingAccounts/0A0A0A-0A0A0A-0A0A0A",
            # "bucketName": "fc-secure-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            # "cloudPlatform": "Gcp",
            # "completedCloneWorkspaceFileTransfer": "2024-09-13T12:59:15.795Z",
            # "createdBy": "yulialovesterra@gmail.com",
            # "createdDate": "2024-09-13T12:59:15.795Z",
            # "googleProject": "terra-a1a1a1a1",
            # "googleProjectNumber": "101010101010",
            # "isLocked": false,
            # "lastModified": "2024-09-13T12:59:15.795Z",
            # "name": "workspace-name",
            # "namespace": "terra-project-name",
            # "state": "Ready",
            # "workflowCollectionName": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            # "workspaceId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            # "workspaceType": "rawls",
            # "workspaceVersion": "v2"
            # }
            workspace_response_json = workspace_response.json()
            data = json.loads(json.dumps(workspace_response_json))

            logging.info(f"Successfully created workspace '{workspace_name}' in billing project '{billing_project_name}'.")

            return data['name']

        except Exception as e:
            logging.info(f"ERROR workspace creation for '{workspace_name}' in billing project '{billing_project_name}'. Error: {e}")
            continue
    else:
        raise Exception(f"Error creating workspace: retries maxed out.")


# DELETE WORKSPACE ACTION
def delete_workspace(billing_project_name, workspace_name, rawls_url, azure_token):
    delete_workspace_url = f"{rawls_url}/api/workspaces/v2/{billing_project_name}/{workspace_name}"
    headers = {
        "Authorization": "Bearer " + azure_token,
        "accept": "application/json"
    }

    delete_response = requests.delete(url=delete_workspace_url, headers=headers)
    if delete_response.status_code != 202:
        raise Exception(f"Error submitting deletion workspace request: {delete_response.text}")

    logging.info(f"Successfully submitted deletion request for workspace '{workspace_name}' in billing project '{billing_project_name}'. Response: {delete_response.text}")

    # sleep for 2 minutes
    logging.info("Sleeping for 2 minutes before polling for workspace status...")
    time.sleep(2 * 60)

    # prevent infinite loop
    poll_count = 16 # 30s x 16 = 8 min

    # poll every 30s to check if workspace was deleted
    workspace_status_url = f"{rawls_url}/api/workspaces/{billing_project_name}/{workspace_name}"
    while poll_count > 0:
        response = requests.get(workspace_status_url, headers=headers)
        status_code = response.status_code

        if status_code == 200:
            logging.info(f"Workspace '{workspace_name}' in billing project '{billing_project_name}' still exists. Sleeping for 30 seconds")
            time.sleep(30)
        elif status_code == 401:
            raise Exception(f"Azure token expired.")
        elif status_code == 404:
            logging.info(f"Workspace '{workspace_name}' in billing project '{billing_project_name}' deleted successfully")
            return
        else:
            raise Exception(f"Something went wrong while workspace deletion. Received status code {status_code}. Error: {response.text}")

        poll_count -= 1

    raise Exception(f"Workspace wasn't deleted within 10 minutes.")


# SHARE WORKSPACE ACTION
def share_workspace_grant_owner(orch_url, billing_project_name, workspace_name, email_to_share, owner_token):
    request_body = [{
        "email": f"{email_to_share}",
        "accessLevel": "OWNER",
        "canShare": True,
        "canCompute": True
    }]

    uri = f"{orch_url}/api/workspaces/{billing_project_name}/{workspace_name}/acl?inviteUsersNotFound=true"
    headers = {
        "Authorization": f"Bearer {owner_token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    response = requests.patch(uri, json=request_body, headers=headers)
    status_code = response.status_code

    if status_code != 200:
        raise Exception(response.text)

    logging.info(f"Successfully shared workspace {workspace_name} with {email_to_share} as OWNER")


# ADD WDL TO GCP WORKSPACE ACTION
def add_wdl_to_gcp_workspace(billing_project_name, workspace_name, wdl_namespace, wdl_name, method_definition_dict, root_entity_type, inputs_dict, outputs_dict, orch_url, token):
    uri = f"{orch_url}/api/workspaces/{billing_project_name}/{workspace_name}/method_configs/{wdl_namespace}/{wdl_name}"
    request_body = {
        "namespace": wdl_namespace,
        "name": wdl_name,
        "rootEntityType": root_entity_type,
        "workspaceName": {
            "name": workspace_name,
            "namespace": billing_project_name
        },
        "methodRepoMethod": method_definition_dict,
        "outputs": outputs_dict,
        "inputs": inputs_dict,
        "prerequisites": {},
        "methodConfigVersion": 1,
        "deleted": False
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }
    response = requests.put(uri, json=request_body, headers=headers)
    status_code = response.status_code

    if status_code != 200:
        raise(Exception(response.text))
    
    logging.info(f"Successfully added wdl method config {wdl_namespace}/{wdl_name} to workspace {billing_project_name}/{workspace_name}")


# SET WORKSPACE BUCKET TTL ACTION
def set_workspace_bucket_ttl(rawls_url, billing_project_name, workspace_name, ttl_days, matches_prefix_list, token):
    """Calls Rawls to set a TTL for a certain number of days (ttl_days) on a specified set of object prefixes (matches_prefix_list).
    To set the TTL on all objects in the bucket, specify an empty list for matches_prefix_list.
    Common values to include in the list of prefixes are:
      - "submissions/"
      - "submissions/intermediates/"
      - "submissions/final-outputs/"
    """
    uri = f"{rawls_url}/api/workspaces/v2/{billing_project_name}/{workspace_name}/settings"
    request_body = [
        {
            "settingType": "GcpBucketLifecycle",
            "config": {
                "rules": [
                    {
                        "action": {
                            "actionType": "Delete"
                        },
                        "conditions": {
                            "age": ttl_days,
                            "matchesPrefix": matches_prefix_list
                        }
                    }
                ]
            }
        }
    ]
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }
    response = requests.put(uri, json=request_body, headers=headers)
    status_code = response.status_code

    if status_code != 200:
        raise(Exception(response.text))
    
    logging.info(f"Successfully set a TTL of {ttl_days} days on objects with prefixes {matches_prefix_list} in workspace {billing_project_name}/{workspace_name}")
