import logging
import time

import boto3
import psycopg
import requests
from trino.dbapi import connect as trino_connect

SR = "http://schema-registry:8081"
CONNECT = "http://connect:8083"
FLINK = "http://jobmanager:8081"
KAFKA = "kafka:9092"
CATALOG = "http://iceberg-rest:8181"
NAMESPACE = "cdc"
JOB_NAME = "cdc-lakehouse"
GROUP = "cdc-lakehouse"
CONNECTOR = "shop-cdc"
TOPIC_PREFIX = "dbserver.public."
TABLES = ["customers", "products", "orders", "order_items", "payments", "type_samples"]
S3 = dict(endpoint_url="http://minio:9000", aws_access_key_id="minio", aws_secret_access_key="minio12345", region_name="us-east-1")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logging.getLogger("pyiceberg.io").setLevel(logging.ERROR)


def log(name):
    return logging.getLogger(name)


def pg(admin=False, autocommit=True):
    user = "postgres" if admin else "shop"
    return psycopg.connect(f"host=postgres dbname=shop user={user} password={user}", autocommit=autocommit)


def trino():
    return trino_connect(host="trino", port=8080, user="cdc", catalog="iceberg", schema=NAMESPACE)


def query(sql, params=None):
    with trino() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall()


def s3():
    return boto3.client("s3", **S3)


def catalog():
    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(
        "rest",
        uri=CATALOG,
        **{
            "s3.endpoint": S3["endpoint_url"],
            "s3.access-key-id": S3["aws_access_key_id"],
            "s3.secret-access-key": S3["aws_secret_access_key"],
            "s3.region": S3["region_name"],
            "s3.path-style-access": "true",
        },
    )


def get(url, **kw):
    r = requests.get(url, timeout=kw.pop("timeout", 10), **kw)
    r.raise_for_status()
    return r.json()


def wait_for(check, timeout, interval=2.0, what="condition"):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            value = check()
            if value:
                return value
        except Exception as e:
            last = e
        time.sleep(interval)
    raise TimeoutError(f"timed out after {timeout}s waiting for {what}" + (f": {last}" if last else ""))
