The `pull_and_send_metrics.py` script pulls workflow execution data from BigQuery and sends it to Dockstore.
It then updates the metadata table to indicate that the data has been sent to Dockstore.

Requires:
- signing into a firecloud account with access to the `broad-dsde-prod-analytics-dev` project
- activating a poetry env, then `poetry install` to install dependencies

Args:
--dry_run: Run in test mode. Does not send metrics to dockstore or update the metadata table.
--login: default Re-authenticate with gcloud
--lookback_window: How many days of metrics to send (default 2)

Example usage:
`python workflow_dockstore_metrics/pull_and_send_metrics.py --dry_run --login --lookback_window 1`