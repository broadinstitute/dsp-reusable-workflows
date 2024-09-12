import argparse
import requests
import logging
import sys
import os
 
# getting the name of the directory
# where this file is present.
current = os.path.dirname(os.path.realpath(__file__))
 
# Getting the parent directory name
# where the current directory is present.
grandparent = os.path.dirname(os.path.dirname(current))
 
# adding the parent directory to 
# the sys.path.
sys.path.append(grandparent)
 
# importing
from workspace_helper import create_gcp_workspace, share_workspace_grant_owner, add_wdl_to_gcp_workspace


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run e2e test for TSPS/Imputation service')
    parser.add_argument('-t', '--user_token', required=False,
        help='token for user to authenticate Terra API calls')
    parser.add_argument('-m', '--tsps_sa_email', required=False,
        help='email of tsps service account to share workspace/billing project with')
    parser.add_argument('--use_real_wdl', action='store_true',
        help='whether to import the real ImputationBeagle wdl or the ImputationBeagleEmpty wdl for testing')
    parser.add_argument('-e', '--env', required=False, default='dev',
        help='environment. e.g. `dev` (default) or bee name `terra-marymorg`')
    parser.add_argument('-p', '--billing_project', required=False,
        help='billing project to create workspace in')
    parser.add_argument('-w', '--workspace_name', required=False,
        help='name of workspace to be created, if left blank will be auto-generated')
    parser.add_argument('-b', '--is-bee', action='store_true',
        help='flag that the environment is a bee')
    args = parser.parse_args()


# Setup configuration
user_token = args.user_token

if args.is_bee:
    env_string = f"{args.env}.bee.envs-terra.bio"
else:
    env_string = f"dsde-{args.env}.broadinstitute.org"

billing_project_name = args.billing_project
workspace_name = args.workspace_name
tsps_sa_email = args.tsps_sa_email

rawls_url = f"https://rawls.{env_string}"
firecloud_orch_url = f"https://firecloud-orchestration.{env_string}" # this doesn't work for BEEs; BEES it's firecloudorch.{env_string}
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


# Check whether workspace exists
def workspace_exists(billing_project_name, workspace_name, rawls_url, token):
    # returns workspace_id if workspace exists, or None if not

    rawls_workspace_api = f"{rawls_url}/api/workspaces/{billing_project_name}/{workspace_name}"
    
    header = {
        "Authorization": "Bearer " + token,
        "accept": "application/json"
    }

    response = requests.get(url=rawls_workspace_api, headers=header)

    status_code = response.status_code

    if status_code != 200:
        return None
    
    response_json = response.json()
    return response_json['workspace']


# ---------------------- Start TSPS Imputation Workspace Setup ----------------------

logging.info("Starting TSPS imputation workspace setup...")

if workspace_name == "":
    workspace_id = None
else:
    # Check if workspace exists already
    logging.info(f"Checking if workspace {billing_project_name}/{workspace_name} exists...")
    workspace_info = workspace_exists(billing_project_name, workspace_name, rawls_url, user_token)
    logging.info(f"Workspace {'already exists' if workspace_info else 'does not yet exist'}")

if not(workspace_info):
    # Create workspace
    logging.info("Creating workspace...")
    workspace_id, workspace_name = create_gcp_workspace(billing_project_name, user_token, rawls_url, workspace_name)

# share created workspace with the tsps service account
if tsps_sa_email:
    logging.info(f"sharing workspace with {tsps_sa_email}")
    share_workspace_grant_owner(firecloud_orch_url, billing_project_name, workspace_name,
                    tsps_sa_email, user_token)




# import imputation method
wdl_namespace = billing_project_name
root_entity_type = "imputation_beagle"

use_real_wdl = True
if use_real_wdl:
    wdl_name = "ImputationBeagle"
    version_or_branch = "TSPS-183_mma_beagle_imputation_hg38"
    method_definition_dict = {
        "methodUri": f"dockstore://github.com%2Fbroadinstitute%2Fwarp%2FImputationBeagle/{version_or_branch}",
        "sourceRepo": "dockstore",
        "methodPath": "github.com/broadinstitute/warp/ImputationBeagle",
        "methodVersion": version_or_branch
    }
else:
    wdl_name = "ImputationBeagleEmpty"
    version_or_branch = "0.0.100"
    method_definition_dict = {
        "methodUri": f"dockstore://github.com%2FDataBiosphere%2Fterra-scientific-pipelines-service%2FImputationBeagleEmpty/{version_or_branch}",
        "sourceRepo": "dockstore",
        "methodPath": "github.com/DataBiosphere/terra-scientific-pipelines-service/ImputationBeagleEmpty",
        "methodVersion": version_or_branch

    }

inputs_dict = {
    f"{wdl_name}.contigs": "this.contigs",
    f"{wdl_name}.genetic_maps_path": "this.genetic_maps_path",
    f"{wdl_name}.multi_sample_vcf": "this.multi_sample_vcf",
    f"{wdl_name}.output_basename": "this.output_basename",
    f"{wdl_name}.ref_dict": "this.ref_dict",
    f"{wdl_name}.reference_panel_path_prefix": "this.reference_panel_path_prefix",
  }

outputs_dict = {
    f"{wdl_name}.chunks_info": "this.chunks_info",
    f"{wdl_name}.imputed_multi_sample_vcf": "this.imputed_multi_sample_vcf",
    f"{wdl_name}.imputed_multi_sample_vcf_index": "this.imputed_multi_sample_vcf_index"
  }

logging.info(f"Adding {wdl_name} ({method_definition_dict['methodPath']}) to workspace")

add_wdl_to_gcp_workspace(billing_project_name, workspace_name, wdl_namespace, wdl_name, method_definition_dict, root_entity_type, inputs_dict, outputs_dict, firecloud_orch_url, user_token)
