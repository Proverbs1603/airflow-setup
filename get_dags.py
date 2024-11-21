import requests.auth
from requests.auth import HTTPBasicAuth

url = "http://localhost:8080/api/v1/dags"

dags = requests.get(url, auth=HTTPBasicAuth("airflow", "airflow"))

for d in dags.json()["dags"]:
    if not d["is_paused"]:
        print(d["dag_id"])