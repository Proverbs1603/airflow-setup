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

'''
Incremental update는 execution_date (almost same with 읽어와야하는 데이터 일자)
을 주면서 실행시키는데 만약 7월 한달간 데이터를 가져와야한다면 실행을 어떻게 해야하는가?
-> airflow dags backfill dag_id -s 2024-07-01 -e 2024-08-01

-- Preparing Option --
start_date부터 시작하지만 end_date은 포함하지 않음.
catchUp = Ture
execution_date을 사용해서 Incremental update가 구현되어 있어야함.
default_args의 'depends_on_past'를 True로 설정시 실행순서를 날짜순으로 할 수 있음. 
데이터 소스 자체가 특정 일자를 기준으로 조회할 수 있어야함.
'''
dag = DAG(
    dag_id = 'MySQL_to_Redshift_v2',
    start_date = datetime(2023,1,1), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 9 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'depends_on_past': True,
    }
)

schema = "tlsdnd1667"
table = "nps"
s3_bucket = "grepp-data-engineering"
s3_key = schema + "-" + table       # s3_key = schema + "/" + table

# airflow의 execution_date를 활용하여 
sql = "SELECT * FROM prod.nps WHERE DATE(created_at) = DATE('{{ execution_date }}')"
print(sql)
mysql_to_s3_nps = SqlToS3Operator(
    task_id = 'mysql_to_s3_nps',
    query = sql,
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    sql_conn_id = "mysql_conn_id",
    aws_conn_id = "aws_conn_id",
    verify = False,
    replace = True,
    pd_kwargs={"index": False, "header": False},
    dag = dag
)

s3_to_redshift_nps = S3ToRedshiftOperator(
    task_id = 's3_to_redshift_nps',
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    schema = schema,
    table = table,
    copy_options=['csv'],
    redshift_conn_id = "redshift_dev_db",
    aws_conn_id = "aws_conn_id",
    method = "UPSERT",
    upsert_keys = ["id"],  # upsert_keys를 primary key의 uniqueness를 보장
    dag = dag
)

mysql_to_s3_nps >> s3_to_redshift_nps