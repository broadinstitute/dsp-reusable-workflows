import argparse
import requests
import logging
import sys
import os
 
# get the name of the directory where this file is present.
current = os.path.dirname(os.path.realpath(__file__))
 
# get the parent directory name where the current directory is present.
grandparent = os.path.dirname(os.path.dirname(current))
 
# add the parent directory to the sys.path.
sys.path.append(grandparent)
 
# import
from workspace_helper import create_gcp_workspace, share_workspace_grant_owner, add_wdl_to_gcp_workspace, set_workspace_bucket_ttl


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Set up Control Workspace for Teaspoons Imputation service')
    parser.add_argument('-t', '--user-token', required=False,
        help='token for user to authenticate Terra API calls')
    parser.add_argument('-m', '--teaspoons-sa-email', required=False,
        help='email of teaspoons service account to share workspace/billing project with')
    parser.add_argument('--wdl-tag-or-branch', required=True, help='github reference (tag or branch) to use for source wdls')
    parser.add_argument('--use-empty-wdl', action='store_true',
        help='whether to import the empty ImputationBeagleEmpty wdl for testing; if not set, defaults to the real ImputationBeagle wdl')
    parser.add_argument('-e', '--env', required=False, default='dev',
        help='environment. e.g. `dev` (default) or bee name `terra-marymorg`')
    parser.add_argument('-b', '--is-bee', action='store_true',
        help='flag that the environment is a bee')
    parser.add_argument('-p', '--billing-project', required=False,
        help='billing project to create workspace in')
    parser.add_argument('-w', '--workspace-name', required=False,
        help='name of workspace to be created, if left blank will be auto-generated')
    parser.add_argument('-a', '--auth-domain', required=True,
                        help='auth domain group name (without @firecloud.org suffix) to use with new workspace')
    parser.add_argument('-s', '--share-with', nargs='*', help='(optional) additional email addresses beyond SA to share workspace with at Owner level')

    args = parser.parse_args()


# Setup configuration
user_token = args.user_token
is_bee = args.is_bee

if is_bee:
    env_string = f"{args.env}.bee.envs-terra.bio"
else:
    env_string = f"dsde-{args.env}.broadinstitute.org"

billing_project_name = args.billing_project
workspace_name = args.workspace_name
teaspoons_sa_email = args.teaspoons_sa_email
emails_to_share_with = args.share_with

auth_domains = [args.auth_domain]

rawls_url = f"https://rawls.{env_string}"
teaspoons_url = f"https://teaspoons.{env_string}"
if is_bee:
    firecloud_orch_url = f"https://firecloudorch.{env_string}" 
else:
    firecloud_orch_url = f"https://firecloud-orchestration.{env_string}"

wdl_tag_or_branch = args.wdl_tag_or_branch

# configure logging format
LOG_FORMAT = "%(asctime)s %(levelname)-8s %(message)s"
LOG_LEVEL = "INFO"
LOG_DATEFORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(
    format=LOG_FORMAT,
    level=getattr(logging, LOG_LEVEL),
    datefmt=LOG_DATEFORMAT,
)


# Check whether workspace exists
def workspace_exists(billing_project_name, workspace_name, rawls_url, token):
    # returns True if workspace exists, or False if not

    rawls_workspace_api = f"{rawls_url}/api/workspaces/{billing_project_name}/{workspace_name}"
    
    header = {
        "Authorization": "Bearer " + token,
        "accept": "application/json"
    }

    response = requests.get(url=rawls_workspace_api, headers=header)

    status_code = response.status_code

    if status_code == 200:
        # workspace exists!
        return True
    elif status_code == 500:
        logging.error(f"Call to check workspace failed with error 500, {response}, exiting")
        exit(1)
    
    return False


# ---------------------- Start Teaspoons Imputation Workspace Setup ----------------------

logging.info(f"Starting GCP Teaspoons Imputation Service Runner Workspace setup in {args.env}...")

# Check if workspace exists already
logging.info(f"Checking if workspace {billing_project_name}/{workspace_name} exists...")
this_workspace_exists = workspace_exists(billing_project_name, workspace_name, rawls_url, user_token)
logging.info(f"Workspace {'already exists' if this_workspace_exists else 'does not yet exist'}")

if not(this_workspace_exists):
    # Create workspace
    logging.info("Creating secure workspace...")
    workspace_name = create_gcp_workspace(billing_project_name, user_token, rawls_url, workspace_name, auth_domains=auth_domains, enhanced_bucket_logging=True)

# share created workspace with the teaspoons service account
if teaspoons_sa_email:
    logging.info(f"sharing workspace with {teaspoons_sa_email}")
    share_workspace_grant_owner(firecloud_orch_url, billing_project_name, workspace_name,
                                teaspoons_sa_email, user_token)
if emails_to_share_with:
    for email_to_share_with in emails_to_share_with:
        logging.info(f"sharing workspace wtih {email_to_share_with}")
        share_workspace_grant_owner(firecloud_orch_url, billing_project_name, workspace_name, email_to_share_with, user_token)

# set TTL on files in submissions directory of workspace bucket
ttl_days = 14  # delete after 2 weeks
matches_prefix_list = ["submissions/", "user-input-files/"]
set_workspace_bucket_ttl(rawls_url, billing_project_name, workspace_name, ttl_days, matches_prefix_list, user_token)

# import methods
wdl_namespace = billing_project_name
root_entity_type = "array_imputation"
use_empty_wdl = args.use_empty_wdl

# import imputation method
wdl_name = "ImputationBeagle"

if use_empty_wdl:
    method_definition_dict = {
        "methodUri": f"dockstore://github.com%2FDataBiosphere%2Fterra-scientific-pipelines-service%2FImputationBeagleEmpty/{wdl_tag_or_branch}",
        "sourceRepo": "dockstore",
        "methodPath": "github.com/DataBiosphere/terra-scientific-pipelines-service/ImputationBeagleEmpty",
        "methodVersion": wdl_tag_or_branch
    }
else:
    method_definition_dict = {
        "methodUri": f"dockstore://github.com%2Fbroadinstitute%2Fwarp%2FImputationBeagle/{wdl_tag_or_branch}",
        "sourceRepo": "dockstore",
        "methodPath": "github.com/broadinstitute/warp/ImputationBeagle",
        "methodVersion": wdl_tag_or_branch
    }

logging.info(f"Adding \"{wdl_name}\" ({method_definition_dict['methodPath']}) to workspace")

add_wdl_to_gcp_workspace(billing_project_name, workspace_name, wdl_namespace, wdl_name, method_definition_dict, root_entity_type, {}, {}, firecloud_orch_url, user_token)

# import quota method
wdl_name = "QuotaConsumed"

use_empty_wdl = args.use_empty_wdl
if use_empty_wdl:
    method_definition_dict = {
        "methodUri": f"dockstore://github.com%2FDataBiosphere%2Fterra-scientific-pipelines-service%2FQuotaConsumedEmpty/{wdl_tag_or_branch}",
        "sourceRepo": "dockstore",
        "methodPath": "github.com/DataBiosphere/terra-scientific-pipelines-service/QuotaConsumedEmpty",
        "methodVersion": wdl_tag_or_branch
    }
else:
    method_definition_dict = {
        "methodUri": f"dockstore://github.com%2Fbroadinstitute%2Fwarp%2FArrayImputationQuotaConsumed/{wdl_tag_or_branch}",
        "sourceRepo": "dockstore",
        "methodPath": "github.com/broadinstitute/warp/ArrayImputationQuotaConsumed",
        "methodVersion": wdl_tag_or_branch
    }

logging.info(f"Adding \"{wdl_name}\" ({method_definition_dict['methodPath']}) to workspace")

add_wdl_to_gcp_workspace(billing_project_name, workspace_name, wdl_namespace, wdl_name, method_definition_dict, root_entity_type, {}, {}, firecloud_orch_url, user_token)
