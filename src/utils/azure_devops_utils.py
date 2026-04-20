# Databricks notebook source
# MAGIC %md
# MAGIC Class to interact with Azure DevOps. Create branches.

# COMMAND ----------

class ManageAzureDevOps():
  def __init__(self, azure_devops_token, organization, project):

    self.azure_devops_token = azure_devops_token
    self.organization = organization
    self.project = project
    self.auth = ("", self.azure_devops_token)
    self.headers = {
      "Authorization": f"Bearer {azure_devops_token}"
    }
    log_info("Azure DevOps Management Initialized")
    
  def create_working_branch(self, repo_id, base_branch = "main", feat_or_bug = "feature", notebook_name = ""):
    # Azure DevOps branch creation
    if notebook_name == "": log_error("Notebook name cannot be empty")
    new_branch = f"{feat_or_bug}/{notebook_name}"

    # Check if branch already exists
    url = f"https://dev.azure.com/{self.organization}/{self.project}/_apis/git/repositories/{repo_id}/refs"
    params = {
        "filter": f"heads/{new_branch}",
        "api-version": "7.1-preview.1"
    }

    response = requests.get(url, auth=self.auth, params=params)
    data = response.json()

    if response.status_code == 200:
        if data['count'] > 0:
            log_warn(f"Branch {new_branch} already exists")
            return new_branch
        else:
            log_info(f"Branch {new_branch} does not exist. It will be created.")
    else:
        log_error(f"Error: {response.status_code}")


    # Get base commit SHA
    r = requests.get(
      f"https://dev.azure.com/{self.organization}/{self.project}/_apis/git/repositories/{repo_id}/refs",
      params={"filter":f"heads/{base_branch}", "api-version":"5.1"},
      auth=self.auth
    )
    commit_sha = r.json()["value"][0]["objectId"]

    # Create new branch
    payload = [{
      "name":f"refs/heads/{new_branch}",
      "oldObjectId":"0000000000000000000000000000000000000000",
      "newObjectId":commit_sha
    }]

    try:
      response = requests.post(
        f"https://dev.azure.com/{self.organization}/{self.project}/_apis/git/repositories/{repo_id}/refs?api-version=5.1",
        json=payload, 
        auth=self.auth
      )
      response.raise_for_status()  # Raises HTTPError if status is 4xx or 5xx
      log_info(f"New branch created")
      return new_branch
    except requests.exceptions.HTTPError as e:
      log_error(f"HTTP error occurred: {e} - {response.text}")
    except Exception as e:
      log_error(f"Unexpected error: {e}")
