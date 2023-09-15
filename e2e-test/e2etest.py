
from helper import *
import os

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

def run_workspace_app_test(cbas, wds_upload, cbas_submit_workflow, test_cloning):

    workspace_manager_url, rawls_url, leo_url = setup(bee_name)
    print("AZURE_TOKEN: ", azure_token)
    print("BEE_NAME: ", bee_name)
    print("BILLING_PROJECT_NAME: ", billing_project_name)
     # WHERE E2E test actually begins
    header = {"Authorization": "Bearer " + azure_token};
    print("HEADER: ", header)

    # create the workspace
    workspace_id, workspace_name = create_workspace(cbas, billing_project_name, header)

    # track to see when the workspace WDS is ready to upload data into them
    # sleep for 5 minutes to allow WDS to start up, if no wds, only sleep 2 minutes to let cbas start up
    if wds_upload:
        time.sleep(200)
    elif cbas_submit_workflow:
        time.sleep(120)
    else:
        print("TEST COMPLETE.")

    if wds_upload:
        print(f"trying to see wds is ready to upload stuff for workspace {workspace_id}")
        wds_url = get_app_url(workspace_id, "wds", azure_token)
        if wds_url == "":
            print(f"wds errored out for workspace {workspace_id}")
        else:
            upload_wds_data(wds_url, workspace_id, "resources/test.tsv", "test", azure_token)

    if cbas_submit_workflow:
        # next trigger a workflow in each of the workspaces, at this time this doesnt monitor if this was succesful or not
        # upload file needed for workflow to run
        upload_wds_data(wds_url, workspace_id, "resources/sraloadtest.tsv", "sraloadtest", azure_token)
        submit_workflow_assemble_refbased(workspace_id, "resources/assemble_refbased.json", azure_token)

    if test_cloning:
        clone_id = clone_workspace(billing_project_name, workspace_name, header)
        wds_url = get_app_url(clone_id, "wds", azure_token)
        check_wds_data(wds_url, clone_id, "test", azure_token)

    print("TEST COMPLETE.")


#Run create-wds-test
run_workspace_app_test(cbas,wds_upload,cbas_submit_workflow,test_cloning)

#Run clone-wds-test
# run_workspace_app_test(cbas,wds_upload,cbas_submit_workflow,True)