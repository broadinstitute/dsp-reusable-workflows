import requests
import json
import uuid
import time
import logging
import shared_variables


# CREATE APP IN WORKSPACE
def create_app(workspace_id, leo_url, app_type, access_scope, azure_token):
    logging.info(f"Creating {app_type} in workspace {workspace_id}...")
    uri = f"{leo_url}/api/apps/v2/{workspace_id}/terra-app-{str(uuid.uuid4())}"
    body = {
        "appType": f"{app_type}",
        "accessScope": f"{access_scope}"
    }
    headers = {
        "Authorization": f"Bearer {azure_token}",
        "accept": "application/json"
    }

    response = requests.post(url=uri, json=body, headers=headers)
    # will return 202 or error
    if response.status_code != 202:
        raise Exception(f"Error creating {app_type} app. Response: {response.text}")

    logging.info(response.text)

# GET APP PROXY URL FROM LEO
def poll_for_app_url(workspaceId, app_type, proxy_url_name, azure_token, leo_url):
    leo_get_app_api = f"{leo_url}/api/apps/v2/{workspaceId}?includeDeleted=false"
    headers = {
        "Authorization": "Bearer " + azure_token,
        "accept": "application/json"
    }

    # prevent infinite loop
    polling_attempts_remaining = 30 # 30s x 30 = 15 min
    sleep_time_seconds = 30
    timeout_limit_minutes = polling_attempts_remaining * sleep_time_seconds / 60
    
    for i in range(0,shared_variables.RETRIES):
        try:
            while polling_attempts_remaining > 0:
                response = requests.get(leo_get_app_api, headers=headers)
                if response.status_code != 200:
                    raise Exception(f"Error fetching apps from Leo: ${response.text}")
                response = json.loads(response.text)

                # Don't run in an infinite loop if you forgot to start the app/it was never created
                if app_type not in [item['appType'] for item in response]:
                    logging.warning(f"{app_type} not found in apps, has it been started?")
                    return ""
                for entries in response:
                    if entries['appType'] == app_type:
                        if entries['status'] == "PROVISIONING":
                            logging.info(f"{app_type} is still provisioning. Sleeping for {sleep_time_seconds} seconds")
                            time.sleep(sleep_time_seconds)
                        elif entries['status'] == 'ERROR':
                            logging.error(f"{app_type} is in ERROR state. Error details: {entries['errors']}")
                            return ""
                        elif entries['proxyUrls'][proxy_url_name] is None:
                            logging.error(f"{app_type} proxyUrls not found: {entries}")
                            return ""
                        else:
                            logging.info(f"{app_type} is in READY state")
                            return entries['proxyUrls'][proxy_url_name]
                polling_attempts_remaining -= 1
        except Exception as e:
            logging.info(f"ERROR polling for app '{app_type}' in workspace '{workspaceId}'. Error: {e}")
            # for retries, shorten the time for polling, since these would be caused by transient errors, not waiting for apps to start
            polling_attempts_remaining = 1
            continue
        else:
            # this will execute if no exception was thrown but none of the return statements were executed
            logging.error(f"App still provisioning or missing after {timeout_limit_minutes} minutes")
            break
    else:
        raise Exception(f"Error polling for app url: retries maxed out.")

    return ""
