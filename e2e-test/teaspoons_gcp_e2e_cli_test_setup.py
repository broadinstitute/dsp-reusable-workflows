import time

from workspace_helper import create_gcp_workspace, delete_workspace, share_workspace_grant_owner, add_wdl_to_gcp_workspace
from helper import create_gcp_billing_project, delete_gcp_billing_project
from teaspoons_helper import create_and_populate_terra_group, update_imputation_pipeline_workspace, \
    ping_until_200_with_timeout, query_for_user_quota_consumed, update_quota_limit_for_user

import os
import logging


# Setup configuration
# The environment variables are injected as part of the e2e test setup which does not pass in any args
admin_token = os.environ.get("ADMIN_TOKEN") # admin user who has access to the terra billing project
user_token = os.environ.get("USER_TOKEN") # the user who will kick off the teaspoons job
user_email = os.environ.get("USER_EMAIL") # the user we will want to increase quota_limit for
 
# e2e test is using the teaspoons qa service account
teaspoons_sa_email = "teaspoons-qa@broad-dsde-qa.iam.gserviceaccount.com"

bee_name = os.environ.get("BEE_NAME")
env_string = bee_name + ".bee.envs-terra.bio"

billing_account_name = os.environ.get("BILLING_ACCOUNT_NAME")
billing_project_name = os.environ.get("BILLING_PROJECT_NAME")
workspace_name = os.environ.get("WORKSPACE_NAME")

rawls_url = f"https://rawls.{env_string}"
firecloud_orch_url = f"https://firecloudorch.{env_string}"
teaspoons_url = f"https://teaspoons.{env_string}"
sam_url = f"https://sam.{env_string}"

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

# ---------------------- Start Teaspoons GCP CLI E2E test SETUP ----------------------
try:
    logging.info("Starting Teaspoons GCP E2E test...")
    logging.info(f"billing project: {billing_project_name}, env_string: {env_string}")

    logging.info(f"checking if Sam is pingable")
    ping_until_200_with_timeout(f"{sam_url}/liveness", 300)
    ping_until_200_with_timeout(f"{sam_url}/status", 300)

    logging.info("sleeping for 5 minutes to let the environment settle.  This is due to transient issues with "
                 "uknownhost exceptions that we've seen with past test runs")
    time.sleep(300)

    # Create Terra billing project
    logging.info("Creating billing project...")
    create_gcp_billing_project(rawls_url, billing_project_name, billing_account_name, admin_token)

    # Create auth domain group and add Teaspoons SA as an admin
    logging.info("Creating auth domain...")
    auth_domain_name = "teaspoons-imputation-e2e-test"
    group_admins = [teaspoons_sa_email]
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

    logging.info("sleeping for 5 minutes to let the environment settle after creating the workspace. This is to deal "
                 "with transient issues related to google batch accounts,")
    time.sleep(300)

    # share created workspace with the teaspoons service account
    logging.info("sharing workspace with teaspoons qa service account")
    share_workspace_grant_owner(firecloud_orch_url, billing_project_name, workspace_name,
                                teaspoons_sa_email, admin_token)

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

    # create a new quota consumed method that teaspoons will run
    logging.info("creating quota consumed method")
    wdl_name = "QuotaConsumed"
    method_definition_dict = {
        "methodUri": f"dockstore://github.com%2FDataBiosphere%2Fterra-scientific-pipelines-service%2FQuotaConsumedEmpty/{wdl_method_version}",
        "sourceRepo": "dockstore",
        "methodPath": "github.com/DataBiosphere/terra-scientific-pipelines-service/QuotaConsumedEmpty",
        "methodVersion": wdl_method_version
    }
    add_wdl_to_gcp_workspace(billing_project_name, workspace_name, wdl_namespace, wdl_name, method_definition_dict, root_entity_type, {}, {}, firecloud_orch_url, admin_token)

    # create a new quota consumed method that teaspoons will run
    logging.info("creating input qc method")
    wdl_name = "InputQC"
    method_definition_dict = {
        "methodUri": f"dockstore://github.com%2FDataBiosphere%2Fterra-scientific-pipelines-service%2FInputQCEmpty/{wdl_method_version}",
        "sourceRepo": "dockstore",
        "methodPath": "github.com/DataBiosphere/terra-scientific-pipelines-service/InputQCEmpty",
        "methodVersion": wdl_method_version
    }
    add_wdl_to_gcp_workspace(billing_project_name, workspace_name, wdl_namespace, wdl_name, method_definition_dict, root_entity_type, {}, {}, firecloud_orch_url, admin_token)

    # use admin endpoint to set imputation workspace info
    logging.info("updating imputation workspace info")
    update_imputation_pipeline_workspace(teaspoons_url, billing_project_name, workspace_name, wdl_method_version, admin_token)

    # query for user quota consumed before running pipeline, expect 0
    assert 0 == query_for_user_quota_consumed(teaspoons_url, user_token)
    
    # update user quota limit to 3000
    update_quota_limit_for_user(sam_url, teaspoons_url, admin_token, user_email, 3000)
    
    logging.info("SETUP COMPLETE")

except Exception as e:
    logging.error(f"Exception(s) occurred during setup. Details: {e}")

    # delete workspace
    logging.info("Starting workspace cleanup as part of e2e test...")
    try:
        delete_workspace(billing_project_name, workspace_name, rawls_url, admin_token)
        logging.info("Workspace cleanup complete")
    # Catch the exception and continue with the test since we don't want cleanup to affect the test results.
    # We can assume that Janitor will clean up the workspace if the test fails
    except Exception as e:
        logging.warning(f"Error cleaning up workspace, test script will continue. Error details: {e}")

    # delete billing project
    logging.info("Starting billing project cleanup as part of e2e test...")
    try:
        delete_gcp_billing_project(rawls_url, billing_project_name, admin_token)
        logging.info("Billing project cleanup complete")
    # Catch the exception and continue with the test since we don't want cleanup to affect the test results.
    # We can assume that Janitor will clean up the billing project if the test fails
    except Exception as e:
        logging.warning(f"Error cleaning up billing project, test script will continue. Error details: {e}")

    # Use exit(1) so that GHA will fail if an exception was found during the test
    exit(1)
