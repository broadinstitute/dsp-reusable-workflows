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
wds_upload=True
test_cloning=False
delete_created_workspace=True

def run_workspace_app_test(wds_upload, test_cloning, delete_created_workspace):

    workspace_manager_url, rawls_url, leo_url = setup(bee_name)
    header = {"Authorization": "Bearer " + azure_token};

    # create the workspace
    workspace_id, workspace_name = create_workspace(billing_project_name, azure_token, rawls_url)

    # track to see when the workspace WDS is ready to upload data into them
    # sleep to allow apps to start up
    if wds_upload:
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

    # no point in testing cloning if upload didn't succeed in first place
    if test_cloning and upload_success:
        clone_id, clone_name = clone_workspace(billing_project_name, workspace_name, header)
        wds_url = poll_for_app_url(clone_id, "WDS", "wds", azure_token, leo_url)
        if wds_url == "":
            logging.error(f"wds errored out for cloned workspace {clone_id}")
        else:
            check_wds_data(wds_url, clone_id, "test", azure_token)
            # Once we've verified the cloned data is present, verify we can upload into the cloned workspace
            # This tsv relies on the presence of the cloned data and also has more data types
            clone_upload_success = upload_wds_data(wds_url, clone_id, "resources/all_data_types.tsv", "data", azure_token)
            assert clone_upload_success
        if delete_created_workspace:
            test_cleanup(billing_project_name, clone_name, azure_token)

    if delete_created_workspace:
        test_cleanup(billing_project_name, workspace_name, azure_token)

    print("TEST COMPLETE.")

# Run create & clone wds test
run_workspace_app_test(wds_upload,True,delete_created_workspace)
