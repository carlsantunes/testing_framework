import pytest
from pyspark.sql import SparkSession
import os

@pytest.fixture(scope="session")
def spark():

    is_databricks = os.environ.get("DATABRICKS_RUNTIME_VERSION") is not None
    
    if is_databricks:
        # Databricks already provides Spark
        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("No active SparkSession found in Databricks")
        yield spark
    else:
        # Local / CI / pytest
        spark = (
            SparkSession
            .builder
            .master("local[1]")
            .appName("pytest-pyspark")
            .getOrCreate()
        )
        yield spark
        spark.stop()
