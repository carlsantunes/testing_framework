# Databricks notebook source
# MAGIC %md
# MAGIC Class to interact with databrick. Create git folders, create notebooks, start clusters, start jobs, etc.

# COMMAND ----------

import requests

# COMMAND ----------

class ManageDatabricks():
  def __init__(self, databricks_token, databricks_instance, atlassian_token, user_email):
    self.databricks_token = databricks_token
    self.databricks_instance = databricks_instance
    self.atlassian_token = atlassian_token
    self.user_email = user_email

    self.cluster_names = {
      '1127-122708-sduum220': 'Shared Expl UC',
      '1017-110510-35o7hj6e': 'Shared Expl UC HR',
      '1003-150401-9z4805bt': 'Shared Expl UC'
    }

    # Set the headers
    self.headers = {
      "Authorization": f"Bearer {databricks_token}",
      "Content-Type": "application/json"
    }
    log_info("Databricks Management Initialized")
    
  # Creates git folder
  def create_git_folder(self, repo_url = "https://dev.azure.com/worten/sinba/_git/databricks-dev", branch = "main", git_folder_suffix = ""):
      provider = "azureDevOpsServices"
      git_folder_prefix = repo_url.rsplit('/', 1)[-1]
      repo_workspace_path = f"/Users/{self.user_email}/{git_folder_prefix}-{git_folder_suffix}"

      payload = {
          "url": repo_url,
          "provider": provider,
          "path": repo_workspace_path,
          "branch": branch
      }

      # Check if git folder with that name already exists
      response = requests.get(
        f'{self.databricks_instance}/api/2.0/workspace/get-status',
        headers=self.headers,
        params={'path': repo_workspace_path}
      )

      if response.status_code == 200:
        log_warn(f"Folder already exists: {response.json()}")
        return response.json()['object_id'], repo_workspace_path
      elif response.status_code == 404:
        log_info("Folder does not yet exist. It will be created")
      else:
        log_error("Error:", response.status_code, response.text)


      response = requests.post(
          f"{self.databricks_instance}/api/2.0/repos",
          headers=self.headers,
          json=payload
      )

      if response.status_code == 200:
        log_info(f"Git folder {repo_workspace_path} created successfully!")
        log_info(response.json())  # details about the repo created
        return response.json()['id'], repo_workspace_path # returns git folder id
      else:
        log_error(f"Failed to create git folder {repo_workspace_path}: {response.status_code}")
        return
    
  def associate_working_branch_git_folder(self, databricks_git_folder_id, new_branch):

    response = requests.get(f"{self.databricks_instance}/api/2.0/repos/{databricks_git_folder_id}", headers=self.headers)

    if response.status_code == 200:
      branch = response.json().get("branch")
      if branch == new_branch:
        log_warn(f"Git folder is already associated with branch {branch}")
        return
    else:
      log_error("Error:", response.status_code, response.text)

    # Update Databricks Repo
    requests.patch(
      f"{self.databricks_instance}/api/2.0/repos/{databricks_git_folder_id}",
      headers=self.headers,
      json={"branch": new_branch}
    ).raise_for_status()
    log_info(f"Git folder {databricks_git_folder_id} updated successfully!")

  #
  def create_notebook(self, notebook_content, notebook_path, language="PYTHON"):
    url = f"{self.databricks_instance}/api/2.0/workspace/get-status"
    params = {"path": notebook_path}

    response = requests.get(url, headers=self.headers, params=params)

    # Check result
    if response.status_code == 200:
      object_type = response.json().get("object_type")
      if object_type == "NOTEBOOK":
        log_warn("Notebook already exists. It will be regenerated.")
      else:
        log_warn(f"Object exists but is not a notebook (type: {object_type})")
    elif response.status_code == 404:
      log_info("Notebook does not yet exists. It will be generated.")
    else:
      log_error(f"Unexpected error: {response.status_code}\n{response.text}")

    # Encode the JSON string to base64 (to safely transport as string)
    encoded_string = base64.b64encode(notebook_content.encode('utf-8')).decode('utf-8')

    # Define the notebook creation payload
    payload = {
        "path": notebook_path,
        "language": language,
        "content": encoded_string,  # Base64 encoded content of the notebook
        "overwrite": True
    }

    # Make the API request to create the notebook
    response = requests.post(
        f"{self.databricks_instance}/api/2.0/workspace/import",
        headers=self.headers,
        data=json.dumps(payload)
    )

    # Check the response
    if response.status_code == 200:
      log_info(f"Notebook created successfully: {notebook_path}")
      return
    else:
      log_error(f"Failed to create notebook: {response.text}")

  def create_json(self, json_content, json_path):
    dbutils.fs.put(json_path, json_content, overwrite=True)
  
  #
  def get_notebook_content(self, notebook_path):
    params = {
      "path": notebook_path,
      "format": "SOURCE"
    }

    # Send API request to get notebook content
    response = requests.get(
      f"{self.databricks_instance}/api/2.0/workspace/export", 
      headers=self.headers, 
      params=params
    )

    # Check API result for errors
    if response.status_code == 200:    
      data = response.json()
      log_info("Notebook successfully read")
    else:
      log_warn(f"Request failed with status {response.status_code}: {response.text}.")
      dev_branch_path = re.sub(r'^.*(?=/Transformation_Notebooks/)', '/Repos/dbw-dev-soi/Databricks-dev', notebook_path)
      if dev_branch_path == notebook_path:
        return
      else:
        log_warn(f" Going to read from main branch")
      
      params = {
        "path": dev_branch_path,
        "format": "SOURCE"
      }

      # Send API request to get notebook content
      response = requests.get(
        f"{self.databricks_instance}/api/2.0/workspace/export", 
        headers=self.headers, 
        params=params
      )

      if response.status_code == 200:    
        data = response.json()
        log_info("Notebook successfully read from main branch")
      else:
        log_warn(f"Request failed with status {response.status_code}: {response.text}")

    # Decode de notebook content
    notebook_decoded = base64.b64decode(data["content"]).decode("utf-8")

    log_info("Notebook successfully decoded")
    log_info(notebook_decoded)
    return notebook_decoded
  
  def aux_replace_widget_defaults(self, notebook_content, full_table_path):
    # Define the patterns to search for
    table_pattern = r'dbutils\.widgets\.text\("table",\s*".*?"\)'
    token_pattern = r'dbutils\.widgets\.text\("token",\s*".*?"\)'

    # Define the replacement strings
    table_replacement = f'dbutils.widgets.text("table", "{full_table_path}")'
    token_replacement = f'dbutils.widgets.text("token", "{self.atlassian_token}")'

    # Replace the patterns in the notebook content
    notebook_content = re.sub(table_pattern, table_replacement, notebook_content)
    notebook_content = re.sub(token_pattern, token_replacement, notebook_content)

    return notebook_content
  
  def generate_qa_utils_notebook_string_format(self, qa_utils_template_string, full_table_name):
    qa_notebook_replaced = self.aux_replace_widget_defaults(qa_utils_template_string, full_table_name)

    log_info(f"QA Utils code for table {full_table_name.split('.')[2]} created in string format")
    log_info(qa_notebook_replaced)
    return qa_notebook_replaced
  
  def start_cluster(self, cluster_id):

    # Check if cluster is already running
    status_response = requests.get(
      f"{self.databricks_instance}/api/2.0/clusters/get",
      headers=self.headers,
      params={"cluster_id": cluster_id}
    )
    status = status_response.json().get("state")
    log_info(f"Cluster status: {status}")
    if status == "RUNNING":
      return

    start_response = requests.post(
        f"{self.databricks_instance}/api/2.0/clusters/start",
        headers=self.headers,
        json={"cluster_id": cluster_id}
    )

    if start_response.status_code == 200:
        log_info("Cluster starting...")
    else:
        log_error(f"Error starting cluster: {start_response.text}")
        sys.exit(1)

    # Wait for cluster to be RUNNING
    # while True:
    #     status_response = requests.get(
    #         f"{self.databricks_instance}/api/2.0/clusters/get",
    #         headers=self.headers,
    #         params={"cluster_id": cluster_id}
    #     )
    #     status = status_response.json().get("state")
    #     log_info(f"Cluster status: {status}")
    #     if status == "RUNNING":
    #         break
    #     elif status in ("TERMINATED", "ERROR", "UNKNOWN"):
    #         log_error("Cluster failed to start")
    #         sys.exit(1)
    #     time.sleep(30)

    # log_info("Cluster is running, now you can run your notebook/job")
    return
  
  def start_job(self, notebook_path, cluster_id):
    notebook_name = notebook_path.rsplit('/', 1)[-1]

    job_config = {
        "run_name": f"{notebook_name} on cluster {self.cluster_names[cluster_id]} ",
        "existing_cluster_id": cluster_id,
        "notebook_task": {
            "notebook_path": notebook_path
        }
    }

    response = requests.post(
        f"{self.databricks_instance}/api/2.1/jobs/runs/submit",
        headers=self.headers,
        json=job_config
    )

    if response.status_code == 200:
      run_id = response.json()['run_id']
      log_info(f"Job submitted successfully with run id {run_id}!")
      
      res = requests.get(
        f"{self.databricks_instance}/api/2.1/jobs/runs/get",
        headers=self.headers,
        params={"run_id": run_id}
      )

      run_page_url = res.json().get("run_page_url")
      log_info(run_page_url)

      return response.json()['run_id']
    else:
      log_error(f"Failed to submit job: {response.status_code} {response.text}")
      return None
      
  def check_run_status(self, run_id):
    response = requests.get(
      f"{self.databricks_instance}/api/2.1/jobs/runs/get",
      headers=self.headers,
      params={"run_id": run_id}
    )
  
    if response.status_code == 200:
      return response.json()
    else:
      log_error(f"Failed to get job result: {response.status_code} {response.text}")
      return None
  
  def has_job_finished(self, run_id):
    while True:
      run_info = self.check_run_status(run_id)
      if run_info is None:
        log_error("Error fetching run status")
        return
      state = run_info.get("state", {})
      life_cycle_state = state.get("life_cycle_state")
      result_state = state.get("result_state")

      log_info(f"Life cycle state: {life_cycle_state}")
      log_info(f"Result state: {result_state}")

      if life_cycle_state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
        log_info("Job finished.")
        if result_state:
          log_info(f"Result: {result_state}")
        return 

      log_info("Job still running, waiting 30 seconds...")
      time.sleep(30)

  import requests

  def get_job_result(self, run_id):
    response = requests.get(
      f"{self.databricks_instance}/api/2.1/jobs/runs/get",
      headers=self.headers,
      params={"run_id": run_id}
    )
    
    if response.status_code == 200:
      run_page_url = response.json()['run_page_url']
      log_info(run_page_url)
      return
    else:
      log_error(f"Failed to get job result: {response.status_code} {response.text}")
      return None
  
