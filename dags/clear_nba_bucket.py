import logging
import shutil
import time
from pprint import pprint

import pendulum

from airflow import DAG
from airflow.decorators import task

import os
from datetime import datetime,date

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator


BUCKET = 'nba_api'
PROJECT_ID = 'geoff-kaufman'
GCS_ACL_ENTITY = 'allUsers'
GCS_ACL_BUCKET_ROLE = "OWNER"
GCS_ACL_OBJECT_ROLE = "OWNER"

log = logging.getLogger(__name__)


with DAG(
    dag_id='clear_nba_bucket',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 3, 13, tz="UTC"),
    catchup=False,
) as dag:

    delete_games = GoogleCloudStorageDeleteOperator(
        task_id="delete_games",
        prefix='games/',
        bucket_name=BUCKET,
    )

    delete_pbp = GoogleCloudStorageDeleteOperator(
        task_id="delete_pbp",
        prefix='play_by_play/',
        bucket_name=BUCKET,
    )
