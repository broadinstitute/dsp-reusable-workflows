import requests
import os
import json
import random
import string
import uuid
import time
import logging


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
    assert response.status_code == 202, f"Error creating {app_type} app. Response: {response.text}"
    logging.info(response.text)

# GET APP PROXY URL FROM LEO
def poll_for_app_url(workspaceId, app_type, proxy_url_name, azure_token, leo_url):
    leo_get_app_api = f"{leo_url}/api/apps/v2/{workspaceId}?includeDeleted=false"
    headers = {"Authorization": "Bearer " + azure_token,
               "accept": "application/json"}

    # prevent infinite loop
    poll_count = 20 # 30s x 20 = 10 min

    while poll_count > 0:
        response = requests.get(leo_get_app_api, headers=headers)
        assert response.status_code == 200, f"Error fetching apps from Leo: ${response.text}"
        logging.info(f"Successfully retrieved details for {app_type} app")
        response = json.loads(response.text)

        # Don't run in an infinite loop if you forgot to start the app/it was never created
        if app_type not in [item['appType'] for item in response]:
            logging.error(f"{app_type} not found in apps, has it been started?")
            return ""
        for entries in response:
            if entries['appType'] == app_type:
                if entries['status'] == "PROVISIONING":
                    logging.info(f"{app_type} is still provisioning. Sleeping for 30 seconds")
                    time.sleep(30)
                elif entries['status'] == 'ERROR':
                    logging.error(f"{app_type} is in ERROR state. Quitting.")
                    return ""
                elif entries['proxyUrls'][proxy_url_name] is None:
                    logging.error(f"{app_type} proxyUrls not found: {entries}")
                    return ""
                else:
                    return entries['proxyUrls'][proxy_url_name]
        poll_count -= 1

    logging.error(f"App still provisioning or missing after 10 minutes, quitting")
    return ""