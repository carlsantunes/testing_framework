from __future__ import annotations
from dataclasses import dataclass, field
from functools import lru_cache
from databricks.sdk.runtime import dbutils


@dataclass(frozen=True)
class Config:
    # --- secrets (never print/log these) ---
    databricks_token: str = field(repr=False)
    atlassian_token: str = field(repr=False)
    azure_devops_token: str = field(repr=False)

    # --- non-secrets / settings ---
    databricks_instance: str
    jira_base_url: str
    jira_project_key: str
    user_email: str
    databricks_dev_repo_url: str
    dlm_flows_repo_url: str

    @classmethod
    def from_secret_scope(cls, scope: str) -> "Config":
        """
        Loads configuration from Databricks secrets.
        Databricks recommends storing sensitive values as secrets and referencing them in code. [1](https://docs.databricks.com/aws/en/security/secrets/)
        """

        # Secrets
        databricks_token = dbutils.secrets.get(scope=scope, key="DATABRICKS_TOKEN")
        atlassian_token = dbutils.secrets.get(scope=scope, key="ATLASSIAN_TOKEN")
        azure_devops_token = dbutils.secrets.get(scope=scope, key="AZURE_DEVOPS_TOKEN")

        # Settings (can also be secrets if you want everything in one place)
        databricks_instance = dbutils.secrets.get(scope=scope, key="DATABRICKS_INSTANCE")
        jira_base_url = dbutils.secrets.get(scope=scope, key="JIRA_BASE_URL")
        jira_project_key = dbutils.secrets.get(scope=scope, key="JIRA_PROJECT_KEY")
        databricks_dev_repo_url = dbutils.secrets.get(scope=scope, key="DATABRICKS_DEV_REPO_URL")
        dlm_flows_repo_url = dbutils.secrets.get(scope=scope, key="DLM_FLOW_REPO_URL")

        # Optional: derive current user (fallback to empty)
        try:
            user_email = (
                dbutils.notebook.entry_point.getDbutils()
                .notebook().getContext().userName().get()
            )
        except Exception:
            user_email = dbutils.secrets.get(scope=scope, key="USER_EMAIL")

        return cls(
            databricks_token=databricks_token,
            atlassian_token=atlassian_token,
            azure_devops_token=azure_devops_token,
            databricks_instance=databricks_instance,
            jira_base_url=jira_base_url,
            jira_project_key=jira_project_key,
            user_email=user_email,
            databricks_dev_repo_url=databricks_dev_repo_url,
            dlm_flows_repo_url=dlm_flows_repo_url,
        )

@lru_cache(maxsize=1)
def get_config(scope: str = "automation-framework") -> Config:
    """Cached config accessor: created once per Python process."""
    return Config.from_secret_scope(scope)