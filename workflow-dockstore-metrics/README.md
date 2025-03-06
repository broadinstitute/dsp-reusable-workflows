The `pull_and_send_metrics.py` script pulls workflow execution data from BigQuery and sends it to Dockstore.
It then updates the metadata table to indicate that the data has been sent to Dockstore.

Requires:
- signing into a firecloud account with access to the `broad-dsde-prod-analytics-dev` project
- activating a poetry env, then `poetry install` to install dependencies

Args:
-t --test: Run in test mode. Does not send metrics to dockstore or update the metadata table.
-l --login: Re-authenticate with gcloud

Example usage:
`python workflow_dockstore_metrics/pull_and_send_metrics.py -t -l`