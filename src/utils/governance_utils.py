# Databricks notebook source
# MAGIC %md
# MAGIC Class to check the quality of documentation with respect to the governance guidelines

# COMMAND ----------

class ManageGovernance():
  def __init__(self, notebook):
    self.nb = notebook

    # Required high-level category order in the table.
    self.CATEGORY_ORDER = [
      "pk",
      "suk",
      "ssk",
      "buk",
      "cod",
      "nme",
      "dsc",
      "hrc",
      "rnk",
      "seq",
      "amt",
      "num",
      "pct",
      "qty",
      "rte",
      "val",
      "txt",
      "flg",
      "dat",
      "dtm",
      "cod_ltz",
      "cod_system",
      "cod_scd_1_hash",
      "cod_scd_2_hash",
      "dtm_scd_latest", 
      "dtm_scd_valid_from", 
      "dtm_scd_valid_to",
      "flg_scd_valid_now",
      "dtm_created_at", 
      "dtm_updated_at", 
      "dsc_scd_source",
      "pak"
    ]

    # These columns are not being yet considered in the 
    self.AUDIT_ORDER_EXACT = [
      "dsc_scd_source",
      "flg_scd_valid_now",
      "flg_deleted_at_source",
      "dtm_scd_valid_from",
      "dtm_scd_valid_to",
      "dtm_created_at",
      "dtm_updated_at"
    ]

    self.FIELD_TYPES = {
      "suk": ["BIGINT"],
      "ssk": ["BIGINT"],
      "buk": ["INT", "INTEGER", "BIGINT", "TINYINT", "SMALLINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "STRING"],
      "cod": ["INT", "INTEGER", "STRING"],
      "nme": ["STRING"],
      "dsc": ["STRING"],
      "rnk": ["INT", "INTEGER", "BIGINT", "TINYINT", "SMALLINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC"],
      "seq": ["INT", "INTEGER", "STRING"],
      "amt": ["INT", "INTEGER", "BIGINT", "TINYINT", "SMALLINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC"],
      "num": ["INT", "INTEGER", "BIGINT", "TINYINT", "SMALLINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC"],
      "pct": ["INT", "INTEGER", "BIGINT", "TINYINT", "SMALLINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC"],
      "qty": ["INT", "INTEGER"],
      "rte": ["INT", "INTEGER", "BIGINT", "TINYINT", "SMALLINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC"],
      "val": ["INT", "INTEGER", "STRING"],
      "txt": ["STRING"],
      "flg": ["TINYINT"],
      "dat": ["DATE"],
      "dtm": ["TIMESTAMP"],
      "pak": ["INT", "INTEGER"]
    }

    log_info(f"Governance Management Initialized for table documentation {self.nb.confluence_url}")


  def check_columns_prefixes_and_order(self):
    """
    Return the category key (e.g., 'suk', 'buk', 'dtm') if the column matches any allowed pattern,
    or None if it doesn't match.
    """
    if self.nb.final_table.type_abv in ('vw'):
      log_info("View does not have a strict column prefix or order policy. It needs to be analysed with business sense")
      return
     
    # Build indices for quick lookup
    category_order = list(self.CATEGORY_ORDER)

    # All tokens (full names *and* prefixes) are available for exact-name matching
    exact_index = {token: i for i, token in enumerate(category_order)}

    # Prefix tokens: by convention, 3-letter codes (adjust if your rule is different)
    prefix_tokens = {t for t in category_order if len(t) == 3}
    prefix_index = {t: exact_index[t] for t in prefix_tokens}

    # treat primary keys specially by forcing them to a specific category index
    pk_anchor_token = "pk"
    pk_anchor_idx = exact_index.get(pk_anchor_token, None)

    order_ok = True
    order_errors = []
    invalid_columns = []

    # Compute (index, col_name, match_type, matched_token) for each column
    indices = []
    for i, col in enumerate(self.nb.final_table.columns):
      # Accept both object with .name and plain strings
      col_name = getattr(col, "name", col)
      pk_flag = getattr(col, "primary_key", "False")

      # 1) Try full-name match
      if col_name in exact_index:
        idx = exact_index[col_name]
        match_type = "full"
        matched_token = col_name
        log_check_pass(f"column {col_name} has a valid full name '{matched_token}'")
      else:
        # 2) Try prefix match
        pref = col_name[:3]
        if pref in prefix_index:
          idx = prefix_index[pref]
          match_type = "prefix"
          matched_token = pref
          log_check_pass(f"column {col_name} has a valid prefix '{matched_token}'")
        else:
          idx = None
          match_type = None
          matched_token = None
          invalid_columns.append(col_name)
          log_check_not_pass(f"column {col_name} does not have a valid prefix or full-name")

      # 3) primary key override (if you really need this)
      # Prefer anchoring to a token in CATEGORY_ORDER (e.g., 'suk') rather than a magic number.
      if idx is not None and str(pk_flag) == "True" and pk_anchor_idx is not None:
        # Only pull PK earlier if the anchor index is earlier than its computed index
        if pk_anchor_idx < idx:
          log_check_pass(f"column {col_name} is primary key; anchoring to '{pk_anchor_token}' position")
          idx = pk_anchor_idx
          match_type = f"{match_type}|pk-anchored"

      indices.append((idx, col_name, match_type, matched_token))

    # If there are invalid columns, we still check order for the valid ones,
    # but we mark the overall result as not OK.
    # Extract only valid indices for order checking
    valid_indices = [(idx, name) for (idx, name, _, _) in indices if idx is not None]

    # Check non-decreasing order among valid indices
    for j in range(1, len(valid_indices)):
      prev_idx, prev_col = valid_indices[j - 1]
      curr_idx, curr_col = valid_indices[j]
      if curr_idx < prev_idx:
        order_ok = False
        order_errors.append(
          f"Order violation: '{curr_col}' appears after '{prev_col}' "
          f"but should not precede it per CATEGORY_ORDER."
        )

    # Report
    if invalid_columns:
      order_ok = False
      log_check_not_pass(f"Invalid columns (no allowed token): {invalid_columns}")

    if order_errors:
      for error in order_errors:
        log_check_not_pass(error)
    else:
      if not invalid_columns:
        log_check_pass("Category order is valid")


  def check_at_least_one_columns_is_primary_key(self):
    if self.nb.final_table.primary_key == 'N/A' or self.nb.final_table.primary_key == []:
      log_check_not_pass(f"No primary key found")
    else:
      log_check_pass(f"Primary key columns are {self.nb.final_table.primary_key}")

  def check_columns_have_valid_data_type(self):
    if self.nb.final_table.type_abv in ('vw'):
      log_info("View does not have a strict column prefix therefore no strict data type")
      return
    
    for col in self.nb.final_table.columns:
      if col.data_type.upper().split("(")[0] in self.FIELD_TYPES[col.name[:3]]:
        log_check_pass(f"Column {col.name} has a valid data type {col.data_type}.")
      else:
        log_check_not_pass(f"Column {col.name} has invalid data type {col.data_type}. Fields that start with <{col.name[:3]}> can only have {self.FIELD_TYPES[col.name[:3]]} data types")
