import logging
import shutil
import time
from pprint import pprint

import pendulum

from airflow import DAG
from airflow.decorators import task

import os
from datetime import datetime,date
from tempfile import gettempdir

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSListObjectsOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


BUCKET = 'nba_api'
PROJECT_ID = 'geoff-kaufman'
GCS_ACL_ENTITY = 'allUsers'
GCS_ACL_BUCKET_ROLE = "OWNER"
GCS_ACL_OBJECT_ROLE = "OWNER"

TEMP_DIR_PATH = gettempdir()
GAMES_GCS_DIR = 'games'
PLAY_BY_PLAY_GCS_DIR = 'play_by_play'

log = logging.getLogger(__name__)


# airflow dags backfill \
#     --start-date 2022-03-01 \
#     --end-date 2022-03-15 \
#     etl_nba_streams


with DAG(
    dag_id='etl_nba_streams',
    # schedule_interval="@daily",
    # schedule_interval=None,
    start_date=pendulum.datetime(2022, 3, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1, # avoid rate limit issues
) as dag:

    check_execution_date = BashOperator(
        task_id="check_execution_date",
        bash_command="echo {{ ( execution_date - macros.timedelta(days=1) ).strftime('%Y%m%d') }} ; echo {{ data_interval_start }}",
    )

    fetch_stream_data = BashOperator(
        task_id="fetch_stream_data",
        bash_command="pip install -r $AIRFLOW_HOME/dags/scripts/requirements.txt; python $AIRFLOW_HOME/dags/scripts/fetch_nba_streams.py {{ ( execution_date - macros.timedelta(days=1) ).strftime('%Y%m%d') }}",
    )


    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET,
        project_id=PROJECT_ID,
        resource={
            "iamConfiguration": {
                "uniformBucketLevelAccess": {
                    "enabled": False,
                },
            },
        },
    )


    upload_games_file = LocalFilesystemToGCSOperator(
        task_id="upload_games_file",
        src=os.path.join(TEMP_DIR_PATH, "games_{{ ( execution_date - macros.timedelta(days=1) ).strftime('%m_%d_%Y') }}.json"),
        dst=os.path.join(GAMES_GCS_DIR, "games_{{ ( execution_date - macros.timedelta(days=1) ).strftime('%m_%d_%Y') }}.json"),
        bucket=BUCKET,
    )


    upload_play_by_play_file = LocalFilesystemToGCSOperator(
        task_id="upload_play_by_play_file",
        src=os.path.join(TEMP_DIR_PATH, "play_by_play_{{ ( execution_date - macros.timedelta(days=1) ).strftime('%m_%d_%Y') }}.json"),
        dst=os.path.join(PLAY_BY_PLAY_GCS_DIR, "play_by_play_{{ ( execution_date - macros.timedelta(days=1) ).strftime('%m_%d_%Y') }}.json"),
        bucket=BUCKET,
    )


    check_execution_date >> fetch_stream_data >> create_bucket >> upload_games_file >> upload_play_by_play_file
