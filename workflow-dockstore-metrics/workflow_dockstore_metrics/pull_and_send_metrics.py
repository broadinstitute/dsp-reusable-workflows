"""
This script pulls workflow execution data from BigQuery and sends it to Dockstore.
It then updates the metadata table to indicate that the data has been sent to Dockstore.

Requires:
 - signing into a firecloud account with access to the broad-dsde-prod-analytics-dev project
 - activating the associated poetry env with `poetry shell`, then poetry install

Args:
    -t --test: Run in test mode. Does not send metrics to dockstore or update the metadata table.
    -l --login: Re-authenticate with gcloud
"""

from google.cloud import bigquery
from google.oauth2 import service_account
import requests
import logging
import os
import argparse

# configure logging format
LOG_FORMAT = "%(asctime)s %(levelname)-8s %(message)s"
LOG_LEVEL = "INFO"
LOG_DATEFORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(
    format=LOG_FORMAT,
    level=getattr(logging, LOG_LEVEL),
    datefmt=LOG_DATEFORMAT,
)

WORKFLOW_DATA_QUERY = '''
WITH workflow_runtime_info AS ( 
  SELECT  WORKFLOW_EXECUTION_UUID AS workflow_id, 
          ARRAY_AGG(IF(METADATA_KEY = "status", METADATA_VALUE, NULL) IGNORE NULLS ORDER BY METADATA_TIMESTAMP DESC)[offset(0)] AS status, 
          TIMESTAMP(ARRAY_AGG(IF(METADATA_KEY = "start", METADATA_VALUE, NULL) IGNORE NULLS ORDER BY METADATA_TIMESTAMP DESC)[offset(0)]) AS workflow_start, 
          TIMESTAMP(ARRAY_AGG(IF(METADATA_KEY = "end", METADATA_VALUE, NULL) IGNORE NULLS ORDER BY METADATA_TIMESTAMP DESC)[offset(0)]) AS workflow_end, 
          ARRAY_AGG(IF(METADATA_KEY = "submittedFiles:workflowUrl", METADATA_VALUE, null) IGNORE NULLS)[offset(0)] AS source_url 
  FROM `broad-dsde-prod-analytics-dev.warehouse.cromwell_metadata` as metadata
  LEFT JOIN `broad-dsde-prod-analytics-dev.warehouse.cromwell_metadata_sent_to_dockstore` as sent on metadata.WORKFLOW_EXECUTION_UUID = sent.WORKFLOW_EXECUTION_ID
  WHERE METADATA_TIMESTAMP > DATETIME_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY) and sent.WORKFLOW_EXECUTION_ID IS NULL
  GROUP BY WORKFLOW_EXECUTION_UUID
  HAVING STATUS != "Running"
) 
SELECT workflow_id, status, workflow_start, workflow_end, TIMESTAMP_DIFF(workflow_end, workflow_start, SECOND) AS workflow_runtime_time, source_url 
FROM workflow_runtime_info
WHERE source_url IS NOT NULL
ORDER BY source_url;
'''

class ExecutionData:
    def __init__(self, query_row: bigquery.table.Row):
        self.workflow_id = query_row.workflow_id
        self.date_executed = query_row.workflow_start
        # allowed statuses: "SUCCESSFUL", "FAILED", "ABORTED"
        self.execution_status ="SUCCESSFUL"  if query_row.status == "Succeeded" else query_row.status.upper()
        # represents duration in seconds in ISO 8601 duration format
        self.execution_time = f"PT{query_row.workflow_runtime_time}S"


class WorkflowData:

    def __init__(self, query_row: bigquery.table.Row):
        self.source_url = query_row.source_url
        self.version = self.source_url.split('/')[5]

        first_execution = ExecutionData(query_row)
        self.workflow_executions = [first_execution]
        self.sent_metric = False

    def add_execution(self, execution: ExecutionData):
        self.workflow_executions.append(execution)

    def send_metrics(self, test: bool):

        formatted_executions = ""
        for execution in self.workflow_executions:
            formatted_executions += f'''{{
                "executionId": "{execution.workflow_id}",
                "dateExecuted": "{execution.date_executed}",
                "executionStatus": "{execution.execution_status}",
                "executionTime":  "{execution.execution_time}",
            }},
            '''

        json_request_body = f'''
        {{
            "runExecutions": [
                {formatted_executions}
            ],
        }}
        '''
        uri = f"https://dockstore.org/api/api/ga4gh/v2/extended/{self.source_url}/versions/{self.version}/executions?platform=TERRA"
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json"
        }

        if test:
            logging.info(f"[TEST MODE] Dockstore request: {json_request_body}")
            status_code = 200
        else:
            response = requests.put(uri, json=json_request_body, headers=headers)
            status_code = response.status_code

        if status_code != 200:
            logging.error(f"Error sending metrics for workflow {self.source_url} to dockstore for version {self.version}, response: {response.text}")
        else:
            logging.info(f"Successfully sent {len(self.workflow_executions)} execution metrics for workflow {self.source_url}, version {self.version} to dockstore")
            self.sent_metric = True


def update_metadata_table(client, workflow_ids: list[str], test: bool):
    update_query = f'''
      INSERT INTO `broad-dsde-prod-analytics-dev.warehouse.cromwell_metadata_sent_to_dockstore` (WORKFLOW_EXECUTION_UUID) 
       VALUES 
       ('{"'), ('".join(workflow_ids)}');
'''
    if test:
        logging.info(f"[TEST MODE] Query to update metadata table: {update_query}")
    else:
        query_job = client.query(update_query)  # API request
        query_result = query_job.result()  # Waits for query to finish
        logging.info(f"Query to update metadata table modified {query_result.num_dml_affected_rows} rows.")

# Perform a query.
def get_and_send_workflow_data(client: bigquery.Client, test: bool):
    query_job = client.query(WORKFLOW_DATA_QUERY)
    rows = query_job.result()
    success_count = 0
    updated_workflows = []

    logging.info(f"Total workflow executions to upload: {rows.total_rows}")

    current_workflow = None

    # Ordered by sourceURL, so we can send metrics for all executions of a workflow version at once
    for row in rows:
        if current_workflow is None:
            current_workflow = WorkflowData(row)
            print("setting first workflow")
        elif row.source_url == current_workflow.source_url:
            current_workflow.add_execution(ExecutionData(row))
        # new sourceURL, so send execution metrics for the previous workflow and reset current workflow
        else:
            current_workflow.send_metrics(test)
            # if dockstore PUT is successful, add to the list of workflows to update in the metadata table
            if current_workflow.sent_metric:
                success_count += len(current_workflow.workflow_executions)
                workflow_ids = [execution.workflow_id for execution in current_workflow.workflow_executions]
                updated_workflows.extend(workflow_ids)
            # reset the current_workflow to the new workflow
            current_workflow = WorkflowData(row)

    # send metrics for the last workflow
    current_workflow.send_metrics(test)
    if current_workflow.sent_metric:
        success_count += len(current_workflow.workflow_executions)
        workflow_ids = [execution.workflow_id for execution in current_workflow.workflow_executions]
        updated_workflows.extend(workflow_ids)

    # log success rate
    success_rate = round((success_count / rows.total_rows), 4) * 100
    logging.info(f"Successfully sent {success_count}/{rows.total_rows} workflow metrics to dockstore: {success_rate}%")
    return updated_workflows

def main(test: bool, login: bool):
    if login:
        os.system("gcloud auth application-default login")
    # client = bigquery.Client(project="broad-dsde-prod-analytics-dev")
    # successfully_sent_workflows = get_and_send_workflow_data(client, test)
    successfully_sent_workflows = ["test_workflow_id1", "test_workflow_id2", "test_workflow_id3"]
    update_metadata_table(successfully_sent_workflows, test)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sends workflow metrics to Dockstore')
    parser.add_argument('-t', '--test', action='store_true', help='Run in test mode')
    parser.add_argument('-l', '--login', action='store_true', help='Re-authenticate with gcloud')
    args = parser.parse_args()
    main(args.test, args.login)

