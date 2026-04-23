# Class with checks (methods) to test if a data asset follows quality standards (table contract)
from pyspark.sql.functions import col, when, sum, min, max, count, to_timestamp, lit, trim, upper, monotonically_increasing_id, coalesce, lead, row_number, size, length, year, month, quarter, weekofyear, dayofmonth, hour, minute, second, countDistinct
from pyspark.sql.functions import struct, sha2, to_json, desc
from pyspark.sql.types import StringType, MapType, StructField
from pyspark.sql.window import Window
import re # Regular expressions
import functools
import time

# custom modules
from src.utils.logging_utils import log_setup_logic, log_info, log_warn, log_error, log_check_not_pass, log_check_pass

log_setup_logic()

def timing(func):
  @functools.wraps(func)
  def wrapper(*args, **kwargs):
    start = time.perf_counter()
    result = func(*args, **kwargs)
    end = time.perf_counter()
    duration = end - start
    log_info(f"{func.__name__} took {duration:.4f} seconds")
    return result
  return wrapper


class ManageQualityAssurance():
  def __init__(self, df, catalog, schema, table):
    """
    Class containing a list of data quality checks that can be performed on a table/view. 
    Checks are either:
      - the general table contract that needs to be valid for every table.
      - profilling analysis.

    Args:
        catalog (string): Catalog name of table to validate.
        schema (string): Schema name of table to validate.
        table (string): Table name of table to validate.

    Example:
        qa = ManageQualityAssurance("default", "my_schema","fac_sls")
    """
    
    self.catalog = catalog
    self.schema = schema
    self.table = table
    self.full_table_name = f"{catalog}.{schema}.{table}"

    # Table type in star schema (table names need to follow convention).
    if 'dim' in self.table:
      self.table_type = 'dim'
    elif 'fac' in self.table:
      self.table_type = 'fact'
    else:
      self.table_type = 'N/A'

    # Get dataframe with table data.
    self.df_table = df

    # Get columns with "(PK)" in their comment (table columns comments need to follow convention).
    # These are the list of primary keys.
    self.pk_columns = [
      field.name
      for field in self.df_table.schema.fields
      if isinstance(field, StructField) and field.metadata.get("comment", "").find("(PK)") != -1
    ]

    # If table is a dimension, it might not have a comment with PK. However, we know that the suk column is the primary key (according to convention).
    if self.pk_columns == [] and self.table_type == 'dim':
      self.pk_columns = [c for c in self.df_table.columns if c.startswith("suk")]
    
    if self.pk_columns == []:
      log_warn(f"Table/view {self.full_table_name} does not have any primary key")

    # Get all columns that start with "suk"
    self.suk_cols = [c for c in self.df_table.columns if c.startswith("suk")]

    # Get all columns that start with "buk"
    self.buk_cols = [c for c in self.df_table.columns if c.lower().startswith("buk")]

    log_info(f"Quality Assurance Management Initialized for table/view {self.full_table_name}")


  def get_df(self, version=None):
    """
    Returns a pyspark dataframe of the table/view being analysed.
    Since we can do time travel, we can get a table's specific version.

    Args:
      version (int): version of the table to return in dataframe format.

    Returns:
      pyspark.sql.dataframe.DataFrame: dataframe of the table/view being analysed.

    Example:
      df = qa.get_df(version=145)
    """
    if version:
      return #return spark.read.format("delta").option("versionAsOf", version).table(self.full_table_name)
    else:
      return self.df_table

  @timing
  def check_table_empty(self):
    """
    Check if a table is empty (with no data).

    Example:
        df = qa.check_table_empty()
    """
    if self.df_table.count() == 0:
      log_check_not_pass(f"Table {self.full_table_name} is empty")
    else:
      log_check_pass(f"Table {self.full_table_name} is not empty")

  @timing
  def check_dim_suk_1_2(self):
    """
    Check if dim has -1 and -2 records.

    Example:
      qa.check_dim_suk_1_2()
    """
    # If table is not a dimension, skip
    if self.table_type != 'dim':
      log_info(f"Table {self.full_table_name} is not a dimension table")
      return

    # Check if table has any suk columns
    if self.suk_cols == []:
      log_warn(f"Table {self.full_table_name} does not have any suk columns")
      return

    # Build aggregation expressions to count -1 and -2 of suk columns
    agg_exprs = []
    for c in self.suk_cols:
      agg_exprs.append(
        sum(
          when(col(c) == -1, 1)
          .otherwise(0)
        )
        .alias(f"{c}_count_-1")
      )
      agg_exprs.append(
        sum(
          when(col(c) == -2, 1)
          .otherwise(0)
        )
        .alias(f"{c}_count_-2")
      )

    # Compute counts for all suk columns at once
    result = self.df_table.agg(*agg_exprs).collect()[0].asDict()

    # Evaluate conditions
    all_valid = all(
      result[f"{c}_count_-1"] == 1 and result[f"{c}_count_-2"] == 1
      for c in self.suk_cols
    )

    if all_valid:
      log_check_pass(f"All suk columns have exactly one -1 and one -2 record")
    else:
      log_check_not_pass(f"Some suk columns DO NOT have exactly one -1 or one -2 record")

    # show details per column
    for c in self.suk_cols:
      if result[f"{c}_count_-1"] == 1 and result[f"{c}_count_-2"] == 1:
        log_info(f"{c}: -1 count = {result[f'{c}_count_-1']}, -2 count = {result[f'{c}_count_-2']}")
      else:
        log_warn(f"{c}: -1 count = {result[f'{c}_count_-1']}, -2 count = {result[f'{c}_count_-2']}")
  
  @timing
  def count_rows(self):
    """
    Count number of rows in table.

    Example:
      num_rows = qa.count_rows()
    """
    count = self.df_table.count()

    if count > 0:
      log_check_pass(f"Table {self.full_table_name} has {count} rows")
    else:
      log_check_not_pass(f"Table {self.full_table_name} has no rows") 

    return
  
  @timing
  def scd_validity_intervals_overlap(self, granularity="day"):
    """
    Checks if for the same buks in dimension tables, the scd validity intervals overlap. 
    These intervals should not overlap for the check to pass.

    Args:
      granularity (string): granularity of the scd validity intervals. Can be 'year', 'quarter', 'month', 'week', 'day', 'hour', 'minute' or 'second'.

    Example:
      qa.scd_validity_intervals_overlap()
    """
    # Validity check only exists for dimension tables
    if self.table_type != 'dim':
      log_info(f"Table {self.full_table_name} is not a dimension table")
      return

    # Check if table has buk columns
    if len(self.buk_cols) == 0:
      log_error(f"Table {self.full_table_name} does not have any buk columns")

    # Check if table has scd validity columns, like it is expected from a dim table
    for c in ["dtm_scd_valid_from", "dtm_scd_valid_to"]:
      if c not in self.df_table.columns:
        log_error(f"Required column '{c}' not found in Table")

    per_buk: Dict[str, Dict[str, Any]] = {}
    union_summary_list = []
    union_examples_list = []

    func_map = {
      "year": year,
      "quarter": quarter,
      "month": month,
      "week": weekofyear,
      "day": dayofmonth,
      "hour": hour,
      "minute": minute,
      "second": second
    }
    # for each buk
    for buk_col in self.buk_cols:
      # Partition by this BUK column.
      partition_cols = [buk_col]
      w = Window.partitionBy(*[col(c) for c in partition_cols]) \
                .orderBy(col("dtm_scd_valid_from").asc(), col("dtm_scd_valid_to").asc())

      
      seq = (
        self.df_table
        .select(
            *(col(c) for c in partition_cols), 
            func_map[granularity](col("dtm_scd_valid_from")).alias("dtm_scd_valid_from"), 
            func_map[granularity](col("dtm_scd_valid_to")).alias("dtm_scd_valid_to")
          )
          .filter(col(buk_col).isNotNull())
          .withColumn("next_from", lead(col("dtm_scd_valid_from")).over(w))
          .withColumn("next_to",   lead(col("dtm_scd_valid_to")).over(w))
      )

      overlaps = seq.filter((col("next_from").isNotNull()) & (col("next_from") <= col("dtm_scd_valid_to"))) # falta validar esta parte

      # Summary per BUK value
      group_exprs = [col(c) for c in partition_cols]
      summary_df = (
          overlaps.groupBy(*group_exprs)
                  .agg(count(lit(1)).alias("overlaps"))
                  .orderBy(*group_exprs)
      )

      has_overlaps = summary_df.take(1) != []

    if summary_df.count() != 0:
      return summary_df
    else:
      log_check_pass(f"No overlaps found")

  @timing
  def suks_represent_unique_rows(self):
    """
    Partition the table by suk, check for duplicates in business columns (exclude technical fields cod_system, cod_scd_1_hash, cod_scd_2_hash, dtm_scd_latest, dtm_scd_valid_from, dtm_scd_valid_to, flg_scd_valid_now, dtm_created_at, dtm_updated_at, dsc_scd_source

    Example:
      qa.suks_represent_unique_rows()
    """
    # Only dimension tables will need a suk to represent a unique row
    if self.table_type != 'dim':
      log_info(f"Table {self.full_table_name} is not a dimension table")
      return
    
    # Default technical/SCD exclusions
    default_exclude = {
      "cod_system",
      "cod_scd_1_hash",
      "cod_scd_2_hash",
      "dtm_scd_latest",
      "dtm_scd_valid_from",
      "dtm_scd_valid_to",
      "flg_scd_valid_now",
      "dtm_created_at",
      "dtm_updated_at",
      "dsc_scd_source"
    }

    if not self.suk_cols:
      log_warn("No SUK columns found. Nothing to check.")
      return

    # Build business columns
    business_cols = [c for c in self.df_table.columns if c not in default_exclude and c not in self.suk_cols]

    if not business_cols:
      log_warn("No business columns remain after exclusion; cannot compare content.")
      return
    
    # dataframe with suk and business columns
    biz_df = self.df_table.select(*self.suk_cols, *[col(c) for c in business_cols])

    # Helper to create business fingerprint safely
    def with_business_fp(frame):
      biz_struct = struct(*(col(c) for c in business_cols))
      return frame.withColumn("business_fingerprint", sha2(to_json(biz_struct), 256))

    # Per-SUK checks (independent)
    for sk in self.suk_cols:
      log_info("Check " + sk)
      # Skip NULLs to avoid grouping NULLs together (treat NULL key as "unknown")
      sub = biz_df.where(col(sk).isNotNull())
      sub = with_business_fp(sub)

      dup_groups = (
        sub.groupBy(col(sk).alias(sk), "business_fingerprint")
          .agg(count(lit(1)).alias("cnt"))
          .filter(col("cnt") > 1)
      )

      unique = {'duplicates': dup_groups}

      summary_df = (
        dup_groups.groupBy(sk)
                  .agg(
                      count(lit(1)).alias("dup_groups"),
                      sum("cnt").alias("dup_rows")
                  )
                  .orderBy(desc("dup_groups"), desc("dup_rows"))
      )

      unique['summary_df'] = summary_df
      return unique
 
  @timing
  def print_suks_1_2(self):
    """
    Display all rows where there is at least one suk column with the value -1 or -2

    Example:
      qa.print_suks_1_2()
    """
    # Check columns that start with "suk"
    if self.suk_cols == []:
      log_warn(f"Table {self.full_table_name} does not have any suk columns")
      return

    # Build a filter condition: any suk column == -1 or -2
    condition = functools.reduce(
      lambda a, b: a | b,
      [col(c).isin(-1, -2) for c in self.suk_cols]
    )

    # Filter rows where any suk column is -1 or -2
    df_filtered_suks = self.df_table.filter(condition)

    # Show them
    return df_filtered_suks

  def distinct_values_for_each_column(self):
    """
    Display all the distinct values for each table column. Values are ordered by absolute frequency.

    Example:
      df = qa.distinct_values_for_each_column()
    """
    distinct_tables = {}  # dictionary to store DataFrames per column
    total_rows = self.df_table.count() # Total number of rows to compute relative frequency

    distincts = []
    for c in self.df_table.columns:
      # Select only the column, get distinct values
      distinct_tables[c] = (
        self.df_table
        .select(c)
        .groupBy(c)
        .count()
        .withColumn(
          "Relative Frequency",
          round((col("count") / lit(total_rows)) * 100, 2)
        )
        .orderBy("count", ascending=False)
      )
      distincts.append({"table_name": c, "dataframe": distinct_tables[c]})

    return distincts

  @timing
  def max_min_values_for_each_column(self):
    """
    Display max and min value for each column in the table.

    Example:
      df = qa.max_min_values_for_each_column()
    """
    # Build aggregation expressions for all columns
    agg_exprs = []
    for c in self.df_table.columns:
      if isinstance(self.df_table.schema[c].dataType, MapType):
        continue
      agg_exprs.append(min(col(c)).alias(f"{c}_min"))
      agg_exprs.append(max(col(c)).alias(f"{c}_max"))

    # Aggregate
    result = self.df_table.agg(*agg_exprs).collect()[0].asDict()

    # Print nicely
    for c in self.df_table.columns:
      if isinstance(self.df_table.schema[c].dataType, MapType):
        continue
      print(f"{c}: min = {result[f'{c}_min']}, max = {result[f'{c}_max']}")
  
  @timing
  def sums(self):
    """
    Sum all rows of all columns that start with 'qty' or 'amt'.

    Example:
      df = qa.sums()
    """
    # Filter columns that start with 'qty' or 'amt'
    target_columns = [c for c in self.df_table.columns if c.startswith("qty") or c.startswith("amt")]

    if target_columns:
      # Create aggregation expressions
      agg_exprs = [sum(col(c)).alias(f"sum_{c}") for c in target_columns]

      # Apply aggregation
      agg_df = self.df_table.agg(*agg_exprs)

      # Show result
      return agg_df
    else:
      log_info("No columns found that start with 'qty' or 'amt'.")
  
  @staticmethod
  def _null_or_blank_expr(df, colname: str, *, trim_strings: bool = True):
    """Return a boolean Column that's True when the value is null or 'blank',
    using String/Map-appropriate rules."""
    dt = df.schema[colname].dataType
    c = col(colname)

    if isinstance(dt, StringType):
        empty = (length(trim(c)) == 0) if trim_strings else (c == "")
        return c.isNull() | empty

    if isinstance(dt, MapType):
        return c.isNull() | (size(c) == 0)

    # Fallback: only null for other types
    return c.isNull()

  @timing
  def null_or_blank(self):
    """
    Sum all rows of all columns that start with 'qty' or 'amt'.

    Example:
      qa.null_or_blank()
    """
    null_blank_counts = self.df_table.select([
      count(when(self._null_or_blank_expr(self.df_table, c), lit(1))).alias(c)
      for c in self.df_table.columns
    ])

    null_blank_counts

    filtered = []
    # Build condition to check for null or blank in any column
    null_or_blank_condition = None
    for c in self.df_table.columns:
      filtered_df = self.df_table.withColumn("flg_null_or_empty", when(self._null_or_blank_expr(self.df_table, c), lit(1))).filter("flg_null_or_empty = 1").limit(100)
        
      # Check if there are any matching rows
      if filtered_df.count() > 0:
        filtered.append(filtered_df)

        filtered.append(filtered_df)
      return {"counts": null_blank_counts, "dataframes": filtered}

  @timing
  def key_nulls(self):
    """
    Check for nulls in primary key columns

    Example:
      df = qa.key_nulls()
    """
    # Check for nulls in those columns
    if self.pk_columns == []:
      log_warn(f"Table {self.full_table_name} does not have any primary key")
      return
    
    for c in self.pk_columns:
      null_rows = self.df_table.filter(col(c).isNull())
      if null_rows.count() > 0:
        log_check_not_pass(f"Rows with NULL in primary key column: {c}")
        return null_rows
      else:
        log_check_pass(f"Primary key column {c} does not have null values")

  @timing
  def key_duplicates(self):
    """
    Check for primary key duplicates.

    Example:
      qa.key_duplicates()
    """
    if self.pk_columns == []:
      log_warn(f"Table {self.full_table_name} does not have any primary key")
      return
    
    df = self.df_table.select(*self.pk_columns)  # minimize data moved
    dup_groups = (
      df.groupBy(self.pk_columns)
        .count()
        .filter(col("count") > 1)
    )

    has_duplicates = not dup_groups.head(1) == []
    
    if has_duplicates:
      log_check_not_pass(f"Table {self.full_table_name} has duplicates")
      return dup_groups  # shows each duplicate key and its frequency
    else:
      log_check_pass(f"Table {self.full_table_name} does not have duplicates")

  @timing
  def describe_history(self):
    """
    Equivalent to DESCRIBE HISTORY

    Example:
      qa.describe_history()
    """
    try:
      return #display(spark.sql(f"describe history {self.full_table_name}"))
    except Exception as e:
      log_check_not_pass(f"Error {e}. Probably because table {self.full_table_name} is not a table. Skipping history check.")

  @timing
  def describe_table(self):
    """
    Equivalent to DESCRIBE EXTENDED.

    Example:
      qa.key_duplicates()
    """
    #display(spark.sql(f"describe extended {self.full_table_name}"))

  @timing
  def records_validity_dim(self):
    """
    Check if, in a dimension table, all records marked with flg_scd_valid_now = 1 have dtm_valid_to = "9999-12-31T23:59:59.999+00:

    Example:
      df = qa.records_validity_dim()
    """
    if self.table_type != 'dim':
      log_warn(f"Table {self.full_table_name} is not a dimension table")
      return
    
    # Filter rows where flg_is_valid_now = 1 but dtm_valid_to != "9999-12-31T23:59:59.999+00:00"
    invalid_rows = self.df_table.filter(
      (col("flg_scd_valid_now") == 1) &
      (col("dtm_scd_valid_to") != to_timestamp(lit("9999-12-31 23:59:59.999999"), "yyyy-MM-dd HH:mm:ss.SSSXXX"))
    )

    # Check if any invalid rows exist
    any_invalid = invalid_rows.count() > 0
    if any_invalid:
      log_check_not_pass("There are rows where flg_is_valid_now = 1 but dtm_valid_to != '9999-12-31 23:59:59.999999'")
    else:
      log_check_pass("There are no rows where flg_is_valid_now = 1 but dtm_valid_to != '9999-12-31 23:59:59.999999'")

    # show the offending rows
    return invalid_rows

  @timing
  def distinct_suk_buk(self):
    """
    Check if each suk always corresponds to the same buk

    Example:
      df = qa.distinct_suk_buk()
    """  
    cols = self.df_table.columns

    # Build suffix → column mapping
    suk_suffix = {c[3:]: c for c in self.suk_cols}  # remove 'suk'
    buk_suffix = {c[3:]: c for c in self.buk_cols}  # remove 'buk'

    # Find common suffixes
    common_suffixes = set(suk_suffix.keys()) & set(buk_suffix.keys())

    # Build the pairs
    pairs = [(suk_suffix[s], buk_suffix[s]) for s in common_suffixes]

    results = {}

    for suk_col, buk_col in pairs:
      inconsistent = (
        self.df_table.groupBy(suk_col)
        .agg(countDistinct(buk_col).alias("distinct_buk"))
        .filter(col("distinct_buk") > 1)
      )

      results[(suk_col, buk_col)] = inconsistent

    flg_inconsistency = False
    for pair, df_inconsistent in results.items():
      suk_col, buk_col = pair
      if df_inconsistent.count() != 0:
        log_check_not_pass(f"Same suk value found for several buk values")
        flg_inconsistency = True
        # display(df_inconsistent)

    if not flg_inconsistency: 
      log_check_pass(f"Each suk value corresponds to one and only one buk value")

  def dim_pk_suk(self):
    
    inconsistent = (
        self.df_table.groupBy(*self.buk_cols, "dtm_scd_valid_from")
        .agg(countDistinct(*self.suk_cols).alias("distinct_suk"))
        .filter(col("distinct_suk") > 1)
    )

    if inconsistent.count() != 0:
      log_check_not_pass(f"Same buk value found for several suk values")
      return inconsistent
    else:
      log_check_pass(f"Each (buk, dtm_scd_valid_from) value corresponds to one and only one suk")

  # def check_buk_flg_is_active_now(self):
  #   # Check that every buk has at least one row with flg_is_active_now in dim.

  # def check_suk_buk_nulls():
  #   # Check table: Os suk e buks nunca podem estar a null ou a vazio. Apresentar isto como check not pass.

  # def check_dtm_valid_to_greater_dtm_valid_from(self):
  #   # Check if dtm_valid_to é sempre maior do que valid_from

  # def check_view_asset(self):
  #   # executar um select à tabela e à respetiva view para ver se dá algum erro.
