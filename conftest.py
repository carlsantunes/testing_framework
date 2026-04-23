import pytest
from pyspark.sql import SparkSession
import os

@pytest.fixture(scope="session")
def spark():
    
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        spark = spark  # provided by Databricks
    else:
        spark = (
            SparkSession
            .builder
            .master("local[1]")
            .appName("pytest-pyspark")
            .getOrCreate()
        )

    yield spark
    spark.stop()
