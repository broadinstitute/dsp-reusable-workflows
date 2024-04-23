import json
import logging
import requests


# Create imputation method in CBAS
def create_cbas_method(cbas_url, workspace_id, method_name, github_url, token):
    request_body = {
        "method_name": method_name,
        "method_source": "GitHub",
        "method_url": f"{github_url}",
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
