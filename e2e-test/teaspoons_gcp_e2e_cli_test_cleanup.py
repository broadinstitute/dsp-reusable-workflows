from workspace_helper import delete_workspace
from helper import delete_gcp_billing_project


import os
import logging


# Cleanup configuration
# The environment variables are injected as part of the e2e test setup which does not pass in any args
admin_token = os.environ.get("ADMIN_TOKEN") # admin user who has access to the terra billing project

billing_project_name = os.environ.get("BILLING_PROJECT_NAME")
workspace_name = os.environ.get("WORKSPACE_NAME")

bee_name = os.environ.get("BEE_NAME")
env_string = bee_name + ".bee.envs-terra.bio"
rawls_url = f"https://rawls.{env_string}"

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

logging.info("Starting workspace cleanup as part of e2e test...")
try:
    delete_workspace(billing_project_name, workspace_name, rawls_url, admin_token)
    logging.info("Workspace cleanup complete")
# Catch the exception and continue with the test since we don't want cleanup to affect the test results.
# We can assume that Janitor will clean up the workspace if the test fails
except Exception as e:
    logging.warning(f"Error cleaning up workspace, script will continue. Error details: {e}")

# delete billing project
logging.info("Starting billing project cleanup as part of e2e test...")
try:
    delete_gcp_billing_project(rawls_url, billing_project_name, admin_token)
    logging.info("Billing project cleanup complete")
# Catch the exception and continue with the test since we don't want cleanup to affect the test results.
# We can assume that Janitor will clean up the billing project if the test fails
except Exception as e:
    logging.warning(f"Error cleaning up billing project, script will continue. Error details: {e}")

logging.info("Cleanup attempts complete")
