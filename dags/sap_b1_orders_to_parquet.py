from datetime import datetime, timedelta
import io
import json
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import boto3


#Get Credentials By Secret Manager
SECRET_NAME = "sap-b1/service-layer/prod"
AWS_REGION = "us-west-1"

SAP_BASE_URL = "https://sapb1.midominio.com:50000/b1s/v1"
S3_BUCKET = "my-data-lake"
S3_PREFIX = "sap-b1/orders"

def get_secret():
    client = boto3.client("secretsmanager", region_name=AWS_REGION)
    resp = client.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(resp["SecretString"])

def login_sap(**context):
    creds = get_secret()
    payload = {
        "CompanyDB": creds["company_db"],
        "UserName": creds["username"],
        "Password": creds["password"]
    }

    session = requests.Session()
    r = session.post(
        f"{SAP_BASE_URL}/Login",
        json=payload,
        timeout=60,
        verify=True
    )
    r.raise_for_status()

    # Save Cookies
    cookies_dict = requests.utils.dict_from_cookiejar(session.cookies)
    context["ti"].xcom_push(key="sap_cookies", value=cookies_dict)

def extract_orders(**context):
    ti = context["ti"]
    cookies_dict = ti.xcom_pull(task_ids="login_sap", key="sap_cookies")

    session = requests.Session()
    session.cookies = requests.utils.cookiejar_from_dict(cookies_dict)

    last_watermark = Variable.get("sap_b1_orders_last_watermark", default_var="2026-01-01")
    results = []
    skip = 0
    page_size = 100

    while True:
        url = (
            f"{SAP_BASE_URL}/Orders"
            f"?$select=DocEntry,DocNum,CardCode,DocDate,UpdateDate,DocTotal"
            f"&$filter=UpdateDate ge '{last_watermark}'"
            f"&$top={page_size}&$skip={skip}"
        )
        r = session.get(url, timeout=120, verify=True)
        r.raise_for_status()
        payload = r.json()

        batch = payload.get("value", [])
        if not batch:
            break

        results.extend(batch)
        skip += page_size

    ti.xcom_push(key="orders_data", value=results)

def write_parquet_to_s3(**context):
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="extract_orders", key="orders_data")

    if not rows:
        return

    df = pd.DataFrame(rows)

    if "DocDate" in df.columns:
        df["DocDate"] = pd.to_datetime(df["DocDate"], errors="coerce")
    if "UpdateDate" in df.columns:
        df["UpdateDate"] = pd.to_datetime(df["UpdateDate"], errors="coerce")

    table = pa.Table.from_pandas(df, preserve_index=False)

    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    execution_date = context["ds"]  # yyyy-mm-dd
    key = f"{S3_PREFIX}/load_date={execution_date}/orders.parquet"

    s3 = S3Hook(aws_conn_id="aws_default")
    s3.load_bytes(
        bytes_data=buffer.getvalue(),
        key=key,
        bucket_name=S3_BUCKET,
        replace=True
    )

def update_watermark(**context):
    Variable.set("sap_b1_orders_last_watermark", context["ds"])

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sap_b1_orders_to_parquet",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["sap", "b1", "parquet", "s3"],
) as dag:

    login = PythonOperator(
        task_id="login_sap",
        python_callable=login_sap,
    )

    extract = PythonOperator(
        task_id="extract_orders",
        python_callable=extract_orders,
    )

    parquet = PythonOperator(
        task_id="write_parquet_to_s3",
        python_callable=write_parquet_to_s3,
    )

    watermark = PythonOperator(
        task_id="update_watermark",
        python_callable=update_watermark,
    )

    login >> extract >> parquet >> watermark