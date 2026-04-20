# Databricks notebook source
import pytest
from src import utils

@pytest.fixture
def qa():
    # create a table
    catalog = "sales_dev"
    schema = "dw"
    table = "pytest_carlos"
    
    table_schema = StructType([
        StructField("buk_test1", IntegerType(), nullable=False),
        StructField("buk_test2", StringType(), nullable=True),
        StructField("amt_", DoubleType(), nullable=True),
        StructField("dtm_created_at", TimestampType(), nullable=True),
    ])
    
    # insert test data
    data = [
        (1, "Alice", 100.5, None),
        (2, "Bob", 200.0, None),
    ]
    df = spark.createDataFrame(data, schema=table_schema)
    df.write.saveAsTable(f"{catalog}.{schema}.{table}")

    return utils.ManageQualityAssurance(catalog, schema, table)

    # drop table
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{table}")

def test_qa(qa):
    assert True
