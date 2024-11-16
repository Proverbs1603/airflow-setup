from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2
import json


dag = DAG(
    dag_id = 'MySQL_to_Redshift',
    start_date = datetime(2024,11,10), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 9 * * *',  # 적당히 조절
    max_active_runs = 1,     # data source의 부하를 줄이기 위해서 1로 설정
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

schema = "tlsdnd1667"
table = "nps"
s3_bucket = "grepp-data-engineering"
s3_key = schema + "-" + table

mysql_to_s3_nps = SqlToS3Operator(
    task_id = 'mysql_to_s3_nps',
    query = "SELECT * FROM prod.nps",
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    sql_conn_id = "mysql_conn_id",
    aws_conn_id = "aws_conn_id",
    verify = False,
    replace = True, #s3에 파일이 존재해도 덮어쓰기
    pd_kwargs={"index": False, "header": False},  #pandas 옵션
    dag = dag
)

s3_to_redshift_nps = S3ToRedshiftOperator(
    task_id = 's3_to_redshift_nps',
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    schema = schema,
    table = table,
    copy_options=['csv'],
    method = 'REPLACE',  #테이블 내용 덮어씌기  [REPLACE, APPEND, UPSERT] 옵션
    redshift_conn_id = "redshift_dev_db",
    aws_conn_id = "aws_conn_id",
    dag = dag
)

mysql_to_s3_nps >> s3_to_redshift_nps