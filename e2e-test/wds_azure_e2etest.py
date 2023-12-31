from helper import *
from app_helper import poll_for_app_url
import os
import time

# Setup configuration
# These values should be injected into the environment before setup
azure_token = os.environ.get("AZURE_TOKEN")
bee_name = os.environ.get("BEE_NAME")
billing_project_name = os.environ.get("BILLING_PROJECT_NAME")
#TODO: Take in arguments from the command line?
cbas=False
wds_upload=True
cbas_submit_workflow=False
test_cloning=False
delete_created_workspace=True

def run_workspace_app_test(cbas, wds_upload, cbas_submit_workflow, test_cloning, delete_created_workspace):

    workspace_manager_url, rawls_url, leo_url = setup(bee_name)
    header = {"Authorization": "Bearer " + azure_token};

    # create the workspace
    workspace_id, workspace_name = create_workspace_with_cromwell_app(cbas, billing_project_name, azure_token)

    # track to see when the workspace WDS is ready to upload data into them
    # sleep to allow apps to start up
    if wds_upload or cbas_submit_workflow:
        logging.info("Sleeping for 30 seconds while apps start up")
        time.sleep(30)
    else:
        print("TEST COMPLETE.")

    upload_success = False

    if wds_upload:
        logging.info(f"trying to see wds is ready to upload to workspace {workspace_id}")
        wds_url = poll_for_app_url(workspace_id, "WDS", "wds", azure_token, leo_url)
        if wds_url == "":
            logging.error(f"wds errored out for workspace {workspace_id}")
        else:
            upload_success = upload_wds_data(wds_url, workspace_id, "resources/test.tsv", "test", azure_token)

    if cbas_submit_workflow:
        # next trigger a workflow in each of the workspaces, at this time this doesnt monitor if this was succesful or not
        # upload file needed for workflow to run
        upload_success = upload_wds_data(wds_url, workspace_id, "resources/sraloadtest.tsv", "sraloadtest", azure_token)
        submit_workflow_assemble_refbased(workspace_id, "resources/assemble_refbased.json", azure_token)

    # no point in testing cloning if upload didn't succeed in first place
    if test_cloning and upload_success:
        clone_id = clone_workspace(billing_project_name, workspace_name, header)
        wds_url = poll_for_app_url(clone_id, "WDS", "wds", azure_token, leo_url)
        check_wds_data(wds_url, clone_id, "test", azure_token)

    if delete_created_workspace:
        test_cleanup(billing_project_name, workspace_name, azure_token)

    print("TEST COMPLETE.")

# Run create & clone wds test
run_workspace_app_test(cbas,wds_upload,cbas_submit_workflow,True,delete_created_workspace)
