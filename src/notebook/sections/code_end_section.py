from typing import List

from src.notebook.cells import NotebookCell
from src.notebook.models import NotebookSpec
from src.notebook.sections.section import Section
from src.utils.cell_helpers import md_cell, py_cell, sql_cell


class EndControlSection(Section):
    def __init__(self, staging_schema: str = "stg"):
        self.staging_schema = staging_schema

    def enabled(self, spec: NotebookSpec) -> bool:
        return True

    def build(self, spec: NotebookSpec) -> List[NotebookCell]:
        return [
            md_cell("## End Control Table Process"),
            py_cell(f"""
            # Obtain Statistics
            table_name = "{spec.final_table.catalog}.{spec.final_table.schema}.{spec.final_table.name}"
            statistics = get_statistics(table_name)

            # Obtain list of days processed
            listDaysProcessed_sql = (
                decrypt_any(spark.read.table("{spec.final_table.catalog}.{self.staging_schema}.{spec.final_table.name}"))
                .select(
                    col("suk_date")
                )
                .distinct()
                .orderBy(
                    col("suk_date")
                )
            )

            listDaysProcessedStr = convertColumnJSONArray(
                listDaysProcessed_sql,
                "suk_date",
                True,
                "days"
            )

            # Leave notebook and provide output statistics to pipeline and log
            exitNotebookOuput(
                processKey,
                processName,
                "OK",
                rowsRead=statistics["rows_read"],
                rowsInserted=statistics["rows_inserted"],
                rowsUpdated=statistics["rows_updated"],
                rowsDeleted=statistics["rows_deleted"],
                listDaysProcessed=listDaysProcessedStr
            )
            """),
        ]