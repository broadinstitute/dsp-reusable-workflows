WORKFLOW_DATA_QUERY = '''
WITH workflows_to_send as (
  select distinct WORKFLOW_EXECUTION_UUID
  FROM `broad-dsde-prod-analytics-dev.warehouse.cromwell_metadata` as metadata
  LEFT JOIN `broad-dsde-prod-analytics-dev.warehouse.cromwell_metadata_sent_to_dockstore` as sent on metadata.WORKFLOW_EXECUTION_UUID = sent.WORKFLOW_EXECUTION_ID
  where METADATA_KEY = "end"
  and METADATA_TIMESTAMP > DATETIME_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_window DAY)
  and sent.WORKFLOW_EXECUTION_ID IS NULL
),
workflow_runtime_info AS ( 
  SELECT  metadata.WORKFLOW_EXECUTION_UUID AS workflow_id, 
          ARRAY_AGG(IF(METADATA_KEY = "status", METADATA_VALUE, NULL) IGNORE NULLS ORDER BY METADATA_TIMESTAMP DESC)[offset(0)] AS status, 
          TIMESTAMP(ARRAY_AGG(IF(METADATA_KEY = "start", METADATA_VALUE, NULL) IGNORE NULLS ORDER BY METADATA_TIMESTAMP DESC)[offset(0)]) AS workflow_start, 
          TIMESTAMP(ARRAY_AGG(IF(METADATA_KEY = "end", METADATA_VALUE, NULL) IGNORE NULLS ORDER BY METADATA_TIMESTAMP DESC)[offset(0)]) AS workflow_end, 
          ARRAY_AGG(IF(METADATA_KEY = "submittedFiles:workflowUrl", METADATA_VALUE, null) IGNORE NULLS)[offset(0)] AS source_url 
  FROM `broad-dsde-prod-analytics-dev.warehouse.cromwell_metadata` as metadata
  INNER JOIN workflows_to_send as to_send on metadata.WORKFLOW_EXECUTION_UUID = to_send.WORKFLOW_EXECUTION_UUID
  WHERE METADATA_TIMESTAMP > DATETIME_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
  GROUP BY metadata.WORKFLOW_EXECUTION_UUID
) 
SELECT workflow_id, status, workflow_start, workflow_end, TIMESTAMP_DIFF(workflow_end, workflow_start, SECOND) AS workflow_runtime_time, source_url 
FROM workflow_runtime_info
WHERE source_url IS NOT NULL
ORDER BY source_url;
'''

INSERT_UPDATED_METRICS_QUERY = f'''
      INSERT INTO `broad-dsde-prod-analytics-dev.warehouse.cromwell_metadata_sent_to_dockstore` (WORKFLOW_EXECUTION_ID, TIMESTAMP) 
       VALUES 
       (@values);
'''
