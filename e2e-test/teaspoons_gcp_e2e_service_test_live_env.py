# This script performs an end-to-end test of the Teaspoons GCP service in a live environment (dev or staging).
# It will use the service to run an imputation pipeline with provided input file and verify that the outputs are correct.

from teaspoons_helper import (
    get_user_quota_details, prepare_imputation_pipeline, upload_file_with_signed_url, start_imputation_pipeline,
    poll_for_imputation_job, download_and_verify_outputs, update_quota_limit_for_user, get_output_signed_urls,
    get_pipeline_details, download_file_from_gcs
)

import os
import logging
import subprocess


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

# Setup configuration

# admin_token = subprocess.check_output(["gcloud", "auth", "print-access-token", "--account=dumbledore.admin@test.firecloud.org"], text=True).strip() # os.environ.get("ADMIN_TOKEN") # admin user who has access to the terra billing project
# user_token = subprocess.check_output(["gcloud", "auth", "print-access-token", "--account=groot.testerson@gmail.com"], text=True).strip()  # os.environ.get("USER_TOKEN") # the user who will kick off the teaspoons job
# user_email = "groot.testerson@gmail.com" # os.environ.get("USER_EMAIL") # the user we will want to increase quota_limit for

# environment variables are injected as part of the e2e test setup which does not pass in any args
admin_token = os.environ.get("ADMIN_TOKEN") # admin user who has can update quota limits for user
user_token = os.environ.get("USER_TOKEN") # the user who will run the teaspoons job
user_email = os.environ.get("USER_EMAIL")
env_name = os.environ.get("ENV_NAME") # dev or staging, used to determine urls and input bucket
input_file_name = os.environ.get("INPUT_VCF_FILE_NAME") # name of input file to be used for test, should be located in the input gs bucket for the environment specified by env_name

# GCS bucket path where input data is stored
input_gs_bucket_name = "teaspoons-testing"
input_gs_file_path = f"e2e-test-input-files/{input_file_name}"


# -----------------------------------------------------------------------------------------------------
# ---------------------- Start Teaspoons GCP Service E2E test in live environment ---------------------
found_exception = False
try:
    logging.info(f"Starting Teaspoons GCP E2E test in **{env_name.upper()}** environment")

    if env_name == "dev":
        sam_url = "https://sam.dsde-dev.broadinstitute.org"
        teaspoons_url = "https://teaspoons.dsde-dev.broadinstitute.org/"
    else:
        sam_url = "https://sam.dsde-staging.broadinstitute.org"
        teaspoons_url = "https://teaspoons.dsde-staging.broadinstitute.org/"

    # get user's current available quota
    # note: calling get_user_quota_details will add an entry in the teaspoons db if one does not already exist
    #       for the user which is needed if we want to update the quota limit later
    user_quota = get_user_quota_details(teaspoons_url, user_token)
    user_quota_available = user_quota['quotaLimit'] - user_quota['quotaConsumed']

    # get quota needed to run array_imputation pipeline
    quota_to_run_pipeline = get_pipeline_details(teaspoons_url, "array_imputation", 1, user_token)['pipelineQuota']['minQuotaConsumed']

    logging.info(f"User's available quota: {user_quota_available}, quota needed to run array_imputation pipeline: {quota_to_run_pipeline}")

    # if needed, update the user's quota limit to be enough to run the pipeline
    if user_quota_available < quota_to_run_pipeline:
        new_quota_limit = quota_to_run_pipeline + user_quota['quotaLimit']
        logging.info(f"User does not have enough quota to run the pipeline. Adding {quota_to_run_pipeline} to user's quota limit")
        update_quota_limit_for_user(sam_url,teaspoons_url, admin_token, user_email, new_quota_limit)

    # download input file locally
    local_input_file_path = "/tmp"
    logging.info(f"Downloading input file from gs://{input_gs_bucket_name}/{input_gs_file_path} to {local_input_file_path}")
    download_file_from_gcs(input_gs_bucket_name, input_gs_file_path, local_input_file_path, user_token)

    # prepare imputation pipeline run
    logging.info("Preparing imputation pipeline run")
    job_id, pipeline_file_inputs = prepare_imputation_pipeline(teaspoons_url, f"/tmp/{input_file_name}", "GHA_e2e_test_output", user_token)

    # upload input file using signed url
    if 'multiSampleVcf' in pipeline_file_inputs:
        input_signed_url = pipeline_file_inputs['multiSampleVcf']['signedUrl']
        upload_file_with_signed_url(input_signed_url, f"/tmp/{input_file_name}")
        logging.info("Successfully uploaded multiSampleVcf input file using signed url")
    else:
        raise Exception("multiSampleVcf input not found in pipeline_file_inputs in prepared run")

    # start pipeline run
    logging.info(f"Starting pipeline run for job ID: {job_id}")
    job_id_from_start_run, result_url = start_imputation_pipeline(job_id, teaspoons_url, user_token)

    assert(job_id == job_id_from_start_run)

    # poll for job until completion
    poll_for_imputation_job(result_url, user_token, sleep_interval_mins=5, total_timeout_mins=90)

    # generate output signed urls
    signed_urls = get_output_signed_urls(teaspoons_url, job_id, user_token)

    # download and verify outputs using signed url
    for output_name, url in signed_urls.items():
        logging.info(f"Attempting to download {output_name} output")
        download_and_verify_outputs(output_name, url)

    logging.info("Test completed successfully")

except Exception as e:
    logging.error(f"Exceptions occurred during test. Details: {e}")
    # Use exit(1) so that GHA will fail if an exception was found during the test
    logging.error("Test failed. Exiting with code 1.")
    exit(1)