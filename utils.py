import json

from pyspark.sql import SparkSession
from sqlalchemy import create_engine


def get_db_config(path: str = 'db_config.json') -> dict:
    with open(path, 'r') as f:
        db_config = json.load(f)
        return db_config


def get_pg_engine(pg_config: dict):
    engine = create_engine(
        f"postgresql+psycopg2://{pg_config['user']}:{pg_config['password']}@"
        f"{pg_config['host']}:{pg_config['port']}/{pg_config['dbname']}?client_encoding=utf8",
    )
    return engine


def get_or_create_spark_session(
    master: str = "local",
    app_name: str = "PySpark PGExample via psycopg2",
) -> SparkSession:
    spark_session = SparkSession.builder.master(master).appName(app_name).getOrCreate()
    spark_session.sparkContext.setLogLevel('WARN')
    return spark_session
