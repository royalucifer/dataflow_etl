#!/usr/bin/env bash
# Basic Setting
GCP_PROJECT=ddd-model-gap
JOB_NAME=etl-ga-view
RUNNER=DataflowRunner
REGION=asia-east1
ZONE=c

# Saving Path
BUCKET=dataflow-etl-pipeline
TEMP_LOCATION=gs://${BUCKET}/temp
STAGING_LOCATION=gs://${BUCKET}/staging
OUTPUT_LOCATION=gs://${BUCKET}/data_avro

# Workers
NUM_WORKERS=5
MAX_NUM_WORKERS=20
WORKER_MACHINE_TYPE=n1-standard-1
AUTOSCALING=THROUGHPUT_BASED

DT="2018-07-01"
until [[ $DT > "2018-09-20" ]]; do
    echo "$DT"
    python /home/chenweisung3621/dataflow/view_pipe.py \
        --date ${DT} \
        --output ${OUTPUT_LOCATION} \
        --job_name ${JOB_NAME} \
        --runner ${RUNNER} \
        --project ${GCP_PROJECT} \
        --temp_location ${TEMP_LOCATION} \
        --staging_location ${STAGING_LOCATION} \
        --region ${REGION} \
        --num_workers ${NUM_WORKERS} \
        --max_num_workers ${MAX_NUM_WORKERS} \
        --worker_machine_type ${WORKER_MACHINE_TYPE} \
        --autoscaling_algorithm ${AUTOSCALING} \
        --setup_file /home/chenweisung3621/dataflow/setup.py
    DT=$(date -d "$DT + 1 day" +%F)
done