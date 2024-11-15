from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def extract(url):
    logging.info(datetime.utcnow())
    res = requests.get(url)
    if res.status_code == 200:
        return res.json()


@task
def transform(data):
    countries_info = [
            {
                "name": country.get("name", {}).get("official", "N/A"),
                "population": country.get("population", "N/A"),
                "area": country.get("area", "N/A")
            }
            for country in data
        ]
    logging.info("Transform ended")
    return countries_info


@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
CREATE TABLE {schema}.{table} (
    name text,
    population int,
    area float
);""")
        for r in records:
            name = r["name"]
            population = r["population"]
            area = r["area"]
            print(name, "-", population, "-", area)
            # 파라미터 바인딩을 사용한 SQL 쿼리
            sql = f"INSERT INTO {schema}.{table} (name, population, area) VALUES (%s, %s, %s)"
            cur.execute(sql, (name, population, area))  # 파라미터로 값 전달
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
    logging.info("load done")


with DAG(
    dag_id='restcountriesDag',
    start_date=datetime(2024, 11, 14),
    schedule='30 6 * * 6',  # 매주 토요일 오전6시 30분 (UTC 기준)
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:

    url = "https://restcountries.com/v3/all"
    schema = 'tlsdnd1667'
    table = 'country'

    countries_info = transform(extract(url))
    load(schema, table, countries_info)