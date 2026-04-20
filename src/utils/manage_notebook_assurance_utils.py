# Databricks notebook source
# MAGIC %md
# MAGIC # Class with checks (methods) to test if a notebook is implemented following the guidelines and best practices

# COMMAND ----------

class ManageNotebookAssurance():
  def __init__(self, notebook_path):
    """
    Class containing a list of checks to confirm notebook follows implementation guidelines.

    Args:
      notebook_path (string): path where notebook to analyse exists.

    Example:
        qa = ManageNotebookAssurance(" ")
    """

    # Mandatory notebook section titles
    self.mandatory_sections = ["## Notebook Objectives:", 
                              "## Invoke Utility Notebook",
                              "## In-Built Function Declaration",
                              "## Widgets Declaration",
                              "## Path Declaration",
                              "#### Initialize Control Table Process",
                              "## User Defined Functions (UDF) Declaration",
                              "#### Table creation in ADLS - fact",
                              "## Read Sources (Files and Inputs)",
                              "## Business Logic and Transformation",
                              "## Insert Data in Stg Table",
                              "## Write Data into Delta Lake",
                              "## View Creation",
                              "## End Control Table Process"]
    
  def check_notebook_follows_template(self, notebook_content):
    """
    Check if all mandatory sections are present in the notebook.

    Args:
      notebook_content (string): notebook content in a string variable.

    Example:
      qa.check_notebook_follows_template(notebook_content=nb)
    """
    for section in self.mandatory_sections:
      if section in notebook_content:
        log_check_pass(f"Section {section} found!")
      else:
        log_check_not_pass(f"Section {section} Not found.")

  def check_prints_displays(self, notebook_content):
    """
    Finds all prints and displays statements. We need to garantee that no testing or unnecessary prints are present in the notebook.

    Args:
        notebook_content (string): notebook content in a string variable.

    Example:
        qa.check_prints_displays(notebook_content=nb)
    """
    if any(keyword in notebook_content for keyword in ("print(", "display(")):
      log_check_not_pass(f"Found prints and displays in the notebook.")
    else:
      log_check_pass(f"No prints or displays found")


  def check_coalesce(
      self,
      notebook_content: str,
      prefixes: tuple[str, ...],
      table_columns: list[str],
  ) -> dict:
      """
      Scan `notebook_content` for coalesce(<column>, -1) (incl. lit(-1)) patterns.
      - Prints all lines where matches occur (with caret markers).
      - Prints a table of identifiers found with the given `prefixes` and their line numbers.
      - Returns a structured coverage result against the provided `table_columns`.

      Returns:
          {
            "ok": bool,
            "target_columns": List[str],
            "matched_columns": List[str],
            "missing_columns": List[str],
            "counts": Dict[str, int],
            "matched_lines": List[int],  # 1-based
          }
      """
      import re
      import bisect
      from collections import Counter

      # -------------------------
      # Prep
      # -------------------------
      prefixes_lower = tuple(p.lower() for p in prefixes)
      targets = [c for c in table_columns if c.lower().startswith(prefixes_lower)]

      text = notebook_content
      lines = text.splitlines()

      # Build absolute line-start offsets for pos -> line mapping
      starts = [0]
      pos = 0
      for ln in text.splitlines(keepends=True):
          pos += len(ln)
          starts.append(pos)

      # -------------------------
      # Compile regex (robust across PySpark/SQL/bare identifiers; ensures default is exactly -1)
      # -------------------------
      IDENT = r'(?:`([^`]+)`|"([^"]+)"|([A-Za-z_][A-Za-z0-9_]*))'
      patt = re.compile(
          rf"""
          (?ix)                                  # i: ignore case, x: verbose
          (?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)?    # optional qualifier before coalesce (e.g., F.coalesce)
          coalesce
          \s*\(
              \s*
              (?:                                 # ---------- FIRST ARG ALTERNATIVES ----------
                  # 1) PySpark: col("name") [ .cast("TYPE") ]?
                  col
                  \s*\(
                      \s* {IDENT} \s*             # capture the inner identifier
                  \)
                  \s*
                  (?: \.\s*cast\s*\(
                          \s*
                          (?: {IDENT} | [A-Za-z_][A-Za-z0-9_]* )  # type name (quoted or bare)
                          (?: \s*\(\s*\d+(?:\s*,\s*\d+)?\s*\) )?   # optional precision/scale
                          \s*
                      \)
                  )?
                |
                  # 2) SQL: cast( [qualifier.]name as TYPE(...) )
                  cast
                  \s*\(
                      \s*
                      (?: [A-Za-z_][A-Za-z0-9_]* \s* \. \s* )?   # optional qualifier
                      {IDENT}                                    # capture the inner identifier
                      \s+ as \s+ [A-Za-z_][A-Za-z0-9_]*
                      (?: \s*\(\s*\d+(?:\s*,\s*\d+)?\s*\) )?
                      \s*
                  \)
                |
                  # 3) SQL: bare identifier (optionally parenthesized), with optional qualifier
                  \(* \s*
                  (?: [A-Za-z_][A-Za-z0-9_]* \s* \. \s* )?
                  {IDENT}                                       # capture the inner identifier
                  \s* \)*
              )
              \s*
              ,                                   # ---------- SECOND ARG ----------
              \s*
              (?:
                  # Exactly -1, not -10 or -1.0
                  (?<![\d.])-\s*1(?![\d.])
                |
                  # or lit(-1) with optional qualifier (e.g., F.lit(-1))
                  (?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)? lit \s* \(\s* (?<![\d.])-\s*1(?![\d.]) \s* \)
              )
              \s*
          \)
          """,
          re.VERBOSE,
      )

      # -------------------------
      # Scan matches, extract column names, and keep positional info
      # -------------------------
      matches = []
      for m in patt.finditer(text):
          # Extract first non-empty capture among all groups (first IDENT for column)
          col = None
          if m.lastindex:
              for gi in range(1, m.lastindex + 1):
                  val = m.group(gi)
                  if val:
                      name = val.strip().strip('`"')
                      if "." in name:
                          name = name.split(".")[-1]
                      col = name
                      break

          s_abs, e_abs = m.start(), m.end()

          # map abs -> 0-based line idx with clamping
          s_idx = bisect.bisect_right(starts, s_abs) - 1
          if s_idx < 0:
              s_idx = 0
          elif s_idx > len(starts) - 2:
              s_idx = len(starts) - 2

          e_idx = bisect.bisect_right(starts, e_abs) - 1
          if e_idx < 0:
              e_idx = 0
          elif e_idx > len(starts) - 2:
              e_idx = len(starts) - 2

          matches.append(
              {
                  "start": s_abs,
                  "end": e_abs,
                  "colname": col,
                  "start_line_idx": s_idx,
                  "end_line_idx": e_idx,
              }
          )

      # -------------------------
      # (1) Print lines with caret markers (always)
      # -------------------------
      matched_lines_1based: list[int] = []
      if not matches:
          print("No coalesce(<col>, -1) matches found.")
      else:
          line_to_markers: dict[int, list[int]] = {}
          affected: set[int] = set()
          for m in matches:
              s_abs = int(m["start"])
              s_line = int(m["start_line_idx"])
              rel = s_abs - starts[s_line]
              line_to_markers.setdefault(s_line, []).append(rel)
              for li in range(int(m["start_line_idx"]), int(m["end_line_idx"]) + 1):
                  affected.add(li)

          print("=== Lines with coalesce(..., -1) matches ===")
          for i in sorted(affected):
              ln_no = i + 1
              line = lines[i] if i < len(lines) else ""
              print(f"{ln_no:>6}: {line}")
              if i in line_to_markers and line:
                  caret_chars = [" "] * len(line)
                  for pos in line_to_markers[i]:
                      if 0 <= pos < len(caret_chars):
                          caret_chars[pos] = "^"



  def check_recreate_view(self, notebook_content):
    """
    In Notebook, check if recreateview is False 

    Example:
      df = qa.check_recreate_view()
    """
    RE_RECREATEVIEW_TRUE = re.compile(
        r'\brecreateview\s*=\s*true\b',
        re.IGNORECASE
    )

    RE_RECREATEVIEW_FALSE = re.compile(
        r'\brecreateview\s*=\s*false\b',
        re.IGNORECASE
    )

    if RE_RECREATEVIEW_FALSE.search(notebook_content):
      log_check_pass(f"Table {self.full_table_name} has recreateview = False")
    elif RE_RECREATEVIEW_TRUE.search(notebook_content):
      log_check_not_pass(f"Table {self.full_table_name} has recreateview = True")
    else:
      log_warn(f"Table {self.full_table_name} does not have recreateview = True or False")
