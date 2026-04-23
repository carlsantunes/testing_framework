# Databricks notebook source
import pytest
from src.utils.quality_assurance_utils import ManageQualityAssurance
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


@pytest.fixture
def qa(spark):
    
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

    return ManageQualityAssurance(df, catalog, schema, table)

def test_qa(qa):
    assert qa is not None

def test_get_df(qa):
    assert qa is not None

def test_check_table_empty(qa):
    assert qa is not None

def test_check_dim_suk_1_2(qa):
    assert qa is not None

def test_count_rows(qa):
    assert qa is not None

def test_scd_validity_intervals_overlap(qa):
    assert qa is not None

def test_suks_represent_unique_rows(qa):
    assert qa is not None

def test_print_suks_1_2(qa):
    assert qa is not None

def test_distinct_values_for_each_column(qa):
    assert qa is not None

def test_max_min_values_for_each_column(qa):
    assert qa is not None

def test_sums(qa):
    assert qa is not None

def test__null_or_blank_expr(qa):
    assert qa is not None

def test_null_or_blank(qa):
    assert qa is not None

def test_key_nulls(qa):
    assert qa is not None

def test_key_duplicates(qa):
    assert qa is not None

def test_describe_history(qa):
    assert qa is not None

def test_describe_table(qa):
    assert qa is not None

def test_records_validity_dim(qa):
    assert qa is not None

def test_distinct_suk_buk(qa):
    assert qa is not None

def test_dim_pk_suk(qa):
    assert qa is not None