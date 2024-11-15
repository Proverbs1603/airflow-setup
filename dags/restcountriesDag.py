from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta

import requests
import logging


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
            sql = f"INSERT INTO {schema}.{table} VALUES ('{name}', '{population}', '{area}')"
            cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
    logging.info("load done")


with DAG(
    dag_id='restcountriesDag',
    start_date=datetime(2022, 10, 6),  # 날짜가 미래인 경우 실행이 안됨
    schedule='0 1 * * *',  # 적당히 조절
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:

    url = "https://restcountries.com/v3/all"
    schema = 'tlsdnd1667'   ## 자신의 스키마로 변경
    table = 'country'

    countries_info = transform(extract(url))
    load(schema, table, countries_info)