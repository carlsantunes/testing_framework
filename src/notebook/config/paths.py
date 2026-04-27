from dataclasses import dataclass

@dataclass
class NotebookPaths:
    control_utils: str = "/Repos/dbw-dev-soi/Databricks-dev/Transformation_Notebooks/Control_Utils_uc"
    hr_decrypted_view_creation: str = "/Repos/dbw-dev-soi/Databricks-dev/Transformation_Notebooks/HR_Analytics/Secure_Views/decrypted_view_creation"
    hr_utils: str = "/Repos/dbw-dev-soi/Databricks-dev/Pre_Ingestion_Framework/hr_utils"
    staging_schema: str = "stg"
