# Class to interact with Atlassian products (Confluence and Jira). Reads documentation, reads and creates Jira issues, etc.



%pip install beautifulsoup4

from src.utils.logging_utils import log_setup_logic, log_info, log_warn, log_error, log_check_not_pass, log_check_pass

from src.utils.notebook_utils import ManageNotebook

from bs4 import BeautifulSoup


class JiraIssue():
  def __init__(self, content):
    self.content = content

  def get_squad(self):
    return self.content['fields']['customfield_14827']['value']
  
  def get_issue_type(self):
    return self.content['fields']['issuetype']['name']
  
  def get_key(self):
    return self.content['key']
  
  def get_subtask_summaries(self):
    subtasks_summary = []
    for subtask in self.content['fields']['subtasks']:
      subtasks_summary.append(subtask['fields']['summary'])
    return subtasks_summary



class ManageAtlassian():
  def __init__(self, atlassian_token, user_email):
    self.atlassian_token = atlassian_token
    self.user_email = user_email
    self.basic_auth = HTTPBasicAuth(user_email, atlassian_token)
    self.headers = {
      "Authorization": f"Bearer {atlassian_token}",
      'Accept': 'application/json',
      "Content-Type": "application/json"
    }
    self.jira_base_url = "https://worten.atlassian.net"
    log_info("Atlassian Management Initialized")

  # Get squad from Jira Issue
  def get_jira_issue_fields(self, jira_issue):
    url = f"{self.jira_base_url}/rest/api/3/issue/{jira_issue}"

    # Make the GET request
    response = requests.get(
      url,
      auth=self.basic_auth,
      headers=self.headers
    )

    # Parse the result
    if response.status_code == 200:
      data = response.json()
      log_info(f"Fields for {jira_issue} retrieved")
      return JiraIssue(data)
    else:
      log_error(f"Failed to fetch issue. Status: {response.status_code}")
      log_error(response.text)
      return
    
  def create_jira_issue(self, payload):
    parent_issue = self.get_jira_issue_fields(payload["fields"]["parent"]["key"])
    
    if payload["fields"]["summary"] in parent_issue.get_subtask_summaries():
      log_error(f"Issue with name ||| {payload['fields']['summary']} ||| already exists as sub-issue of {parent_issue.get_key()}")

    # API URL to create issue
    url = f"{self.jira_base_url}/rest/api/3/issue"

    # Send request
    response = requests.post(url, headers=self.headers, auth=self.basic_auth, data=json.dumps(payload))

    if response.status_code == 201:
      log_info(f"{payload['fields']['issuetype']['name']} created successfully!")
      log_info(f"{payload['fields']['issuetype']['name']} key: {response.json()['key']}")
    else:
      log_error(f"Failed to create {payload['fields']['issuetype']['name']}: {response.json()}")

  def edit_jira_issue(self, jira_issue_id, payload):
    parent_issue = self.get_jira_issue_fields(payload["fields"]["parent"]["key"])
    
    if payload["fields"]["summary"] in parent_issue.get_subtask_summaries():
      log_warn(f"Issue with name ||| {payload['fields']['summary']} ||| already exists as sub-issue of {parent_issue.get_key()}. It is going to be edited.")

    # API URL to edit issue
    url = f"{self.jira_base_url}/rest/api/3/issue/{jira_issue_id}"

    # Send request
    response = requests.put(url, headers=self.headers, auth=self.basic_auth, data=json.dumps(payload))

    if response.status_code == 204:
      log_info(f"{payload['fields']['issuetype']['name']} edited successfully!")
      log_info(f"{payload['fields']['issuetype']['name']} key: {response.status_code, response.text}")
    else:
      log_error(f"Failed to create {payload['fields']['issuetype']['name']}: {response.status_code, response.text}")
  
  # Read tables's confluence page content
  def get_page(self, confluence_url):
    
    # get confluence page url
    page_id = confluence_url.split("/")[-2]
    confluence_api_url = f"https://worten.atlassian.net/wiki/rest/api/content/{page_id}?expand=body.storage"
    log_info(f"Corresponding Confluence Page URL: {confluence_api_url}")

    try:
      response = requests.get(url=confluence_api_url, headers=self.headers, auth=self.basic_auth)
      response.raise_for_status()
      log_info("Successfully fetched Confluence content for table")
    except requests.exceptions.RequestException as e:
      log_error(f"Error fetching Confluence content: {e}")
      raise RuntimeError(f"Error fetching Confluence content: {e}")

    #
    response_content = json.loads(response.text) 
    page_title = response_content['title']
    page_body = response_content['body']['storage']['value']
    confluence_content = BeautifulSoup(page_body, 'html.parser')
    log_info("Confluence content parsed")

    return page_title, confluence_content
  


  def _fetch_example_row(self, confluence_url):

    # ---- Step 1: Get attachments for the page ----
    page_id = confluence_url.split("/")[-2]
    attachments_url = f"{self.jira_base_url}/wiki/rest/api/content/{page_id}/child/attachment"
    response = requests.get(attachments_url, auth=self.basic_auth, headers=self.headers)

    if response.status_code != 200:
      print(f"Failed to fetch attachments: {response.status_code} - {response.text}")
      log_error("")

    attachments = response.json().get("results", [])
    if not attachments:
      print("No attachments found on this page.")
      log_error("")

    
    # ---- Step 2: Find CSV attachments ----
    csv_attachments = [att for att in attachments if att["title"].endswith(".csv")]
    if not csv_attachments:
        print("No CSV files found among attachments.")
        log_error("")

    # ---- Step 3: Download each CSV ----
    att = csv_attachments[0]
    download_link = att["_links"]["download"]
    file_url = f"{self.jira_base_url}/wiki/{download_link}"

    file_response = requests.get(file_url, auth=self.basic_auth, headers=self.headers)
    if file_response.status_code != 200:
      print(f"Failed to download CSV: {file_response.status_code}")
      log_error("")

    csv_content = file_response.content.decode("utf-8")
    
    # Split the string into lines
    lines = csv_content.split('\n')

    # Get the second line
    second_line = lines[1].replace('\r', '') if len(lines) > 1 else log_error("")

    return second_line  # Output: Second line

    

  #
  def map_design_to_notebook(self, page_title, page_body, confluence_url):

    #check_table_design_template(confluence_tables)

    # Retrieve title
    notebook_type = page_title.split('-')[0].strip().upper()
    log_info(f"Processing requirements for a {notebook_type} table")

    #
    confluence_tables = page_body.find_all('table')

    # Retrieve data found in the tables of the confluence page to a list of lists
    table_data_list = []
    for table in confluence_tables:
      table_data = []
      for row in table.find_all('tr'):
        cells = [cell.get_text(strip=True) for cell in row.find_all(['td', 'th'])]
        table_data.append(cells)
      table_data_list.append(table_data)

    log_info("Confluence content parsed into a list of lists type")

    if notebook_type in ("DW CL", "CONSOLIDATION LOGIC"):
      log_info("Processing DW CL requirements")
      table_data_list.insert(0, [[[1,2],[1,2]], [[1,2],[1,2]]])
      notebook_name = table_data_list[1][7][1]
    
      squad = table_data_list[1][3][1].split("_")[0]
      
      source_tables_list = []
      for row in table_data_list[1][11:]:
        if row[0] == "Orchestration": # end of source tables
          break
        else:
          table_full_path = row[0].split(':')[-1].split('.')
          name = table_full_path[2]
          schema = table_full_path[1]
          catalog = table_full_path[0].replace("{dev/pp/prd}", "dev")
          condition = row[1].replace(" in ", ".isin()")
          source_tables_list.append(SimpleTable(name, schema, catalog, condition))

      name = table_data_list[1][1][1]
      schema = table_data_list[1][4][1]
      catalog = table_data_list[1][3][1].split("_")[0] + "_dev"
      description = table_data_list[1][2][1]
    
      # final table columns
      column_list = []
      for row in table_data_list[2][2:]:
        if not any(char in row[0] for char in [" ", ":"]):
          if "general_data" in row[0]:
            continue
          print(row[2])
          pk = True if "(PK)" in row[2] else False
          flg_to_encrypt = True if squad.upper() == "PEOPLE" else False
          column_list.append(Column(name=row[0], data_type=row[1], description=row[2], primary_key=pk, flg_to_encrypt=flg_to_encrypt))
    
      # PK
      # Use regular expression to find the substring
      match = re.search(r'Using:(.*?)When matched(?:\s+then)?\s+update:', table_data_list[2][2][7])

      pattern = r'(suk_|ssk_|buk_|cod_|nme_|dsc_|hrc_|rnk_|seq_|amt_|num_|pct_|qty_|rte_|val_|txt_|flg_|dat_|dtm_|pak_|rn).*?(?=(suk_|ssk_|buk_|cod_|nme_|dsc_|hrc_|rnk_|seq_|amt_|num_|pct_|qty_|rte_|val_|txt_|flg_|dat_|dtm_|pak_|rn)|$)'
      
      # Check if a match is found and extract the substring
      if match:
        substring = match.group(1).strip()
      else:
        log_warn("No match found in Merge Logic section")
        substring = ''

      matches = re.findall(pattern, substring, re.DOTALL)

      # Each match is a tuple, the full block starts with group[0]
      pk = []
      for match in re.finditer(pattern, substring, re.DOTALL):
        pk.append(match.group())

      # scd type
      if table_data_list[1][5][0].replace(" ", "") in 'SCDType(OnlyDimensions)':
        if table_data_list[1][5][1] == 'N/A':
          scd_type = 'N/A (Table is not a dimension)'
        else:
          scd_type = table_data_list[1][5][1]
      else:
        log_error("Table design data is not in the expected format")
    
      # table_columns_to_update
      # Use regular expression to find the substring
      match = re.search(r'When matched(?:\s+then)?\s+update:(.*?)When not matched:', table_data_list[2][2][7])

      # Check if a match is found and extract the substring
      if match:
        substring = match.group(1).strip()
      else:
        log_warn("No match found in Merge Logic section")
        substring = ''

      

      # Each match is a tuple, the full block starts with group[0]
      columns_to_update = []
      for match in re.finditer(pattern, substring, re.DOTALL):
        columns_to_update.append(match.group())

      if 'dtm_updated_at' not in columns_to_update:
        columns_to_update.append('dtm_updated_at')
        
      final_table = ComplexTable(name, schema, catalog, description, column_list, pk, scd_type, columns_to_update)

    elif notebook_type == "PRE-ING HR":
      log_info("Processing PRE-ING HR requirements")
      notebook_name = table_data_list[2][4][1]
      table_name = table_data_list[2][0][1]
      schema = table_data_list[2][3][1].split('.')[1]
      catalog = table_data_list[2][3][1].split("_")[0] + "_dev"
      description = table_data_list[2][1][1]
      squad = table_data_list[2][3][1].split("_")[0]

      csv_column_list = []
      validation = []
      for i, row in enumerate(table_data_list[1][1:]):
        if not any(char in row[0] for char in [" ", ":"]):
          name=row[0]
          data_type='string'
          validation.append({'rule_description': row[2], 'error_message': row[3]})
        else:
          validation.append({'rule_description': row[0], 'error_message': row[1]})

        if i+2 >= len(table_data_list[1]) or not any(char in table_data_list[1][i+2][0] for char in [" ", ":"]):
          desc = 'N/A'
          pk = 'N/A'
          for r in table_data_list[3][1:]:
            if r[3] == name:
              desc = r[6]
              pk = "True" if r[5] == 'PK' else "False"
              break
          csv_column_list.append(Column(name=name, data_type=data_type, description=desc, primary_key=pk, validation=validation))
          validation = []

      csv_source_table = ComplexTable(table_name, schema, catalog, description, csv_column_list)
      source_tables_list = [csv_source_table]

      schema = table_data_list[2][2][1].split('.')[1]
      catalog = table_data_list[2][2][1].split("_")[0] + "_dev"
      
      # PK
      # Use regular expression to find the substring
      match = re.search(r'Using:(.*?)When matched update:', table_data_list[3][1][9])

      # Check if a match is found and extract the substring
      if match:
        substring = match.group(1).strip()
      else:
        log_error("No match found")

      pattern = r'(suk|buk|dat_|cod_|nme_|dsc_|hrc_|rnk_|seq_|amt_|num_|pct_|qty_|rte_|val_|txt_|flg_|dat_|dtm_|pak_|rn).*?(?=(suk|buk|dat_|cod_|nme_|dsc_|hrc_|rnk_|seq_|amt_|num_|pct_|qty_|rte_|val_|txt_|flg_|dat_|dtm_|pak_|rn)|$)'
      matches = re.findall(pattern, substring, re.DOTALL)

      # Each match is a tuple, the full block starts with group[0]
      pk = []
      for match in re.finditer(pattern, substring, re.DOTALL):
        pk.append(match.group())

      # table_columns_to_update
      # Use regular expression to find the substring
      match = re.search(r'When matched update:(.*?)When not matched:', table_data_list[3][1][9])

      # Check if a match is found and extract the substring
      if match:
        substring = match.group(1).strip()
      else:
        log_error("In confluence page no match found in Merge Column")

      pattern = r'(suk_|ssk_|buk_|cod_|nme_|dsc_|hrc_|rnk_|seq_|amt_|num_|pct_|qty_|rte_|val_|txt_|flg_|dat_|dtm_|pak_|rn).*?(?=(suk_|ssk_|buk_|cod_|nme_|dsc_|hrc_|rnk_|seq_|amt_|num_|pct_|qty_|rte_|val_|txt_|flg_|dat_|dtm_|pak_|rn)|$)'

      # Each match is a tuple, the full block starts with group[0]
      columns_to_update = []
      for match in re.finditer(pattern, substring, re.DOTALL):
        columns_to_update.append(match.group())

      if 'dtm_updated_at' not in columns_to_update:
        columns_to_update.append('dtm_updated_at')

      column_list = []
      for row in table_data_list[3][1:]:
        if not any(char in row[3] for char in [" ", ":"]):
          primary_key = "True" if row[5] == 'PK' else "False"
          column_list.append(Column(name=row[3], data_type=row[4], description=row[6], primary_key=primary_key))

      scd_type = 'N/A (Table is not a dimension)'
      final_table = ComplexTable(table_name, schema, catalog, description, column_list, pk, scd_type, columns_to_update)
    elif notebook_type == "WDL CSAS" or notebook_type == "WDL SSO":
      log_info("Processing WDL CSAS requirements")
      notebook_name = table_data_list[2][7][1]
      sql_notebook_name = 'dlm_dw.' + notebook_name + '.sql'
      table_name = table_data_list[2][1][1]
      schema = table_data_list[2][3][1].split('.')[1]
      catalog = table_data_list[2][3][1].split('.')[0].replace("{dev/pp/prd}", "dev")
      description = table_data_list[2][1][1]
      squad = "SSO"
      example_csv_row = self._fetch_example_row(confluence_url)


      csv_column_list = []
      validation = []
      for i, row in enumerate(table_data_list[1][1:]):
        if not any(char in row[0] for char in [" ", ":"]) or row[0].upper() == 'ALL FILE':
          name = row[0]
          data_type='string'
          validation.append({'rule_description': row[2], 'error_message': row[3]})
        else:
          if len(row) == 1:
            row.append('N/A')
          validation.append({'rule_description': row[0], 'error_message': row[1]})

        if i+2 >= len(table_data_list[1]) or not any(char in table_data_list[1][i+2][0] for char in [" ", ":"]):
          desc = 'N/A'
          pk = 'N/A'
          for r in table_data_list[3][1:]:
            if r[3] == name:
              desc = r[6]
              pk = "True" if r[5] == 'PK' else "False"
              break
          csv_column_list.append(Column(name=name, data_type=data_type, description=desc, primary_key=pk, validation=validation))
          validation = []

      csv_source_table = ComplexTable(table_name, schema=None, catalog=None, description=description, column_list=csv_column_list)
      source_tables_list = [csv_source_table]
      
      if table_data_list[3][1][9] != "Replace All":
        # PK
        # Use regular expression to find the substring
        match = re.search(r'Using:(.*?)When matched update:', table_data_list[3][1][9])

        # Check if a match is found and extract the substring
        if match:
          substring = match.group(1).strip()
        else:
          log_error("No match found")
  
        pattern = r'(suk|buk|dat_|cod_|nme_|dsc_|hrc_|rnk_|seq_|amt_|num_|pct_|qty_|rte_|val_|txt_|flg_|dat_|dtm_|pak_|rn).*?(?=(suk|buk|dat_|cod_|nme_|dsc_|hrc_|rnk_|seq_|amt_|num_|pct_|qty_|rte_|val_|txt_|flg_|dat_|dtm_|pak_|rn)|$)'
        matches = re.findall(pattern, substring, re.DOTALL)

        # Each match is a tuple, the full block starts with group[0]
        pk = []
        for match in re.finditer(pattern, substring, re.DOTALL):
          pk.append(match.group())

        # table_columns_to_update
        # Use regular expression to find the substring
        match = re.search(r'When matched update:(.*?)When not matched:', table_data_list[3][1][9])

        # Check if a match is found and extract the substring
        if match:
          substring = match.group(1).strip()
        else:
          log_error("In confluence page no match found in Merge Column")
        
        pattern = r'(suk_|ssk_|buk_|cod_|nme_|dsc_|hrc_|rnk_|seq_|amt_|num_|pct_|qty_|rte_|val_|txt_|flg_|dat_|dtm_|pak_|rn).*?(?=(suk_|ssk_|buk_|cod_|nme_|dsc_|hrc_|rnk_|seq_|amt_|num_|pct_|qty_|rte_|val_|txt_|flg_|dat_|dtm_|pak_|rn)|$)'

        # Each match is a tuple, the full block starts with group[0]
        columns_to_update = []
        for match in re.finditer(pattern, substring, re.DOTALL):
          columns_to_update.append(match.group())

        if 'dtm_updated_at' not in columns_to_update:
          columns_to_update.append('dtm_updated_at')

      else:
        pk = []
      
      column_list = []
      for row in table_data_list[3][1:]:
        if not any(char in row[3] for char in [" ", ":"]):
          if row[5] == 'PK':
            primary_key = "True"
            pk.append(row[3])
          else:
            primary_key = "False"
          column_list.append(Column(name=row[3], data_type=row[4], description=row[6], primary_key=primary_key))

      if table_data_list[3][1][9] == "Replace All":
        columns_to_update = column_list
        
      scd_type = 'N/A (Table is not a dimension)'
      final_table = ComplexTable(table_name, schema, catalog, description, column_list, pk, scd_type, columns_to_update)
      return WDLNotebook(notebook_name,
                        sql_notebook_name,
                        squad,
                        source_tables_list,
                        final_table,
                        notebook_type,
                        confluence_url,
                        self.user_email,
                        flow_owner='CSAS',
                        example_csv_row = example_csv_row)
    else:
      log_error("There is no processing logic in Atlassian Utils for this table type")

    if squad == "people":
      squad = "People"
    elif squad in ("sales", "SSO"):
      squad = "SSO"
    elif squad == "general":
      squad = "SSO"
    else:
      log_warn("Catalog is not sales or people. Squad defaulted to SSO.")
      squad = "SSO"

    notebook_classes = {
        "DW CL": DWCLNotebook,
        "CONSOLIDATION LOGIC": DWCLNotebook,
        "PRE-ING HR": PreIngNotebook,
    }

    notebook_subclass = notebook_classes[notebook_type]

    return notebook_subclass(notebook_name,
            squad,
            source_tables_list,
            final_table,
            notebook_type,
            confluence_url,
            self.user_email)
