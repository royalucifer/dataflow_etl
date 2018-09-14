#!/usr/bin/env bash
# Basic Setting
GCP_PROJECT=ddd-model-gap
JOB_NAME=etl-test
RUNNER=DataflowRunner
REGION=asia-east1
ZONE=c

# Saving Path
BUCKET=dataflow-gap-etl
TEMP_LOCATION=gs://${BUCKET}/tmp
STAGING_LOCATION=gs://${BUCKET}/staging

# Workers
NUM_WORKERS=3
MAX_NUM_WORKERS=20
WORKER_MACHINE_TYPE=n1-standard-1
AUTOSCALING=THROUGHPUT_BASED

python /home/chenweisung3621/dataflow/test.py \
    --date 2018-07-25 \
    --output ${TEMP_LOCATION} \
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