import json

from pyspark.sql import SparkSession, DataFrame


def get_db_config(path: str = 'db_config.json') -> dict:
    with open(path, 'r') as f:
        db_config = json.load(f)
        return db_config


def get_or_create_spark_session(
    path: str = '/usr/local/postgresql-42.4.0.jar',
    master: str = 'local',
    app_name: str = 'PySpark_Postgres',
) -> SparkSession:
    spark_session = SparkSession.builder.config(
        'spark.jars', path,
    ).master(master).appName(app_name).getOrCreate()
    spark_session.sparkContext.setLogLevel('WARN')
    return spark_session


def create_dataframe(
    spark_session: SparkSession,
    pg_config: dict,
    table_name: str,
) -> DataFrame:
    url = f"jdbc:postgresql://{pg_config['host']}:{pg_config['port']}/{pg_config['dbname']}"
    options = {
        'url': url,
        'driver': 'org.postgresql.Driver',
        'dbtable': table_name,
        'user': pg_config['user'],
        'password': pg_config['password'],
    }
    df = spark_session.read.format('jdbc').options(**options).load()
    return df


# Using pandas and SQLAlchemy engine

# from pandas import read_sql
# from sqlalchemy import create_engine
# def get_pg_engine(pg_config: dict):
#     engine = create_engine(
#         f"postgresql+psycopg2://{pg_config['user']}:{pg_config['password']}@"
#         f"{pg_config['host']}:{pg_config['port']}/{pg_config['dbname']}?client_encoding=utf8",
#     )
#     return engine


# def get_or_create_spark_session(
#     master: str = 'local',
#     app_name: str = 'PySpark PGExample via psycopg2',
# ) -> SparkSession:
#     spark_session = SparkSession.builder.master(master).appName(app_name).getOrCreate()
#     spark_session.sparkContext.setLogLevel('WARN')
#     return spark_session


# def create_dataframe(engine, spark_session: SparkSession, query: str) -> DataFrame:
#     return spark_session.createDataFrame(read_sql(query, engine))
