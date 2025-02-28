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
import click
from queries import WORKFLOW_DATA_QUERY, INSERT_UPDATED_METRICS_QUERY
import datetime
import configparser
import ast

# configure logging format
LOG_FORMAT = "%(asctime)s %(levelname)-8s %(message)s"
LOG_LEVEL = "INFO"
LOG_DATEFORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(
    format=LOG_FORMAT,
    level=getattr(logging, LOG_LEVEL),
    datefmt=LOG_DATEFORMAT,
)

class ExecutionData:
    ''''
    Class to store execution data for a single workflow run
    '''
    def __init__(self, query_row: bigquery.table.Row):
        self.workflow_id = query_row.workflow_id
        self.date_executed = query_row.workflow_start
        # allowed statuses: "SUCCESSFUL", "FAILED", "ABORTED"
        self.execution_status ="SUCCESSFUL"  if query_row.status == "Succeeded" else query_row.status.upper()
        # represents duration in seconds in ISO 8601 duration format
        self.execution_time = f"PT{query_row.workflow_runtime_time}S"


class WorkflowData:
    ''''
    Class to store workflow data on the version level, and send it to dockstore
    '''

    def __init__(self, query_row: bigquery.table.Row):
        self.source_url = query_row.source_url
        self.version = self.source_url.split('/')[5]

        first_execution = ExecutionData(query_row)
        self.workflow_executions = [first_execution]
        self.sent_metric = False

    def add_execution(self, execution: ExecutionData):
        self.workflow_executions.append(execution)

    def send_metrics(self, test: bool, dockstore_url: str, dockstore_headers: dict):

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
        uri = f"{dockstore_url}{self.source_url}/versions/{self.version}/executions?platform=TERRA"

        if test:
            logging.info(f"[TEST MODE] Dockstore request: {json_request_body}")
            status_code = 200
        else:
            response = requests.put(uri, json=json_request_body, headers=dockstore_headers)
            status_code = response.status_code

        if status_code != 200:
            logging.error(f"Error sending metrics for workflow {self.source_url} to dockstore for version {self.version}, response: {response.text}")
        else:
            logging.info(f"Successfully sent {len(self.workflow_executions)} execution metrics for workflow {self.source_url}, version {self.version} to dockstore")
            self.sent_metric = True


def update_metadata_table(client, workflow_ids: list[str], test: bool):
    timestamp_datetime = datetime.datetime.now().timestamp()
    values = f"', {timestamp_datetime}), ('".join(workflow_ids)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("values", "STRING", values)
        ]
    )
    if test:
        logging.info(f"[TEST MODE] Values to insert to cromwell_metadata_sent_to_dockstore table: {values}")
    else:
        query_job = client.query(INSERT_UPDATED_METRICS_QUERY, job_config)  # API request
        query_result = query_job.result()  # Waits for query to finish
        logging.info(f"Query to update metadata table modified {query_result.num_dml_affected_rows} rows.")

def get_and_send_workflow_data(client: bigquery.Client, dry_run: bool, lookback_window: int, config_parser: configparser.ConfigParser) -> list[str]:
    '''
    Query BigQuery for workflow execution data, send it to dockstore, and keep track of whether the metrics were sent
    Note: Sends metrics for all executions of a workflow version at once

    :param client: BigQuery client, intialized with the project
    :param test: whether to run in test mode or actually send the metrics to dockstore
    :param lookback_window: How many days of metadata to retrieve
    :param config_parser: ConfigParser object with the dockstore settings
    :return: List of workflow_ids that were successfully sent to dockstore
    '''
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("lookback_window", "INT64", lookback_window)
        ]
    )
    query_job = client.query(WORKFLOW_DATA_QUERY, job_config)
    rows = query_job.result()
    success_count = 0
    updated_workflows = []

    logging.info(f"Total workflow executions to upload: {rows.total_rows}")

    # get dockstore settings from config file
    dockstore_url = config_parser.get('Dockstore', 'url')
    dockstore_headers = ast.literal_eval(config_parser.get('Dockstore', 'headers'))

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
            current_workflow.send_metrics(dry_run, dockstore_url, dockstore_headers)
            # if dockstore PUT is successful, add to the list of workflows to update in the metadata table
            if current_workflow.sent_metric:
                success_count += len(current_workflow.workflow_executions)
                workflow_ids = [execution.workflow_id for execution in current_workflow.workflow_executions]
                updated_workflows.extend(workflow_ids)
            # reset the current_workflow to the new workflow
            current_workflow = WorkflowData(row)

    # send metrics for the last workflow
    current_workflow.send_metrics(dry_run)
    if current_workflow.sent_metric:
        success_count += len(current_workflow.workflow_executions)
        workflow_ids = [execution.workflow_id for execution in current_workflow.workflow_executions]
        updated_workflows.extend(workflow_ids)

    # log success rate
    success_rate = round((success_count / rows.total_rows), 4) * 100
    logging.info(f"Successfully sent {success_count}/{rows.total_rows} workflow metrics to dockstore: {success_rate}%")
    return updated_workflows

# configure click args
@click.command()
@click.option('--dry_run', is_flag=True, help='Perform a dry run, will print any requests that alter data instead of sending them')
@click.option('--login', is_flag=True, help='Re-authenticate with gcloud')
@click.option('--lookback_window', default=2, help='Lookback window for querying metadata table, in days')
def main(dry_run: bool, login: bool, lookback_window: int):
    config_parser = configparser.ConfigParser()
    config_parser.read('config.ini')
    if login:
        os.system("gcloud auth application-default login")
    client = bigquery.Client(project=config_parser.get('General', 'bigQuery_project_id'))
    successfully_sent_workflows = get_and_send_workflow_data(client, dry_run, lookback_window, config_parser)
    update_metadata_table(client, successfully_sent_workflows, dry_run)


if __name__ == '__main__':
    main()

