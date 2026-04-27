from typing import List

from src.notebook.cells import NotebookCell
from src.notebook.models import NotebookSpec
from src.notebook.sections.section import Section
from src.utils.cell_helpers import md_cell, py_cell, sql_cell


class ReadSourcesSection(Section):
    def enabled(self, spec: NotebookSpec) -> bool:
        return bool(spec.source_tables)

    def build(self, spec: NotebookSpec) -> List[NotebookCell]:
        all_blocks = []

        for src_tb in spec.source_tables:
            block_lines = [
                f"# Read data from {src_tb.catalog}.{src_tb.schema}.{src_tb.name}",
                f"df_{src_tb.name} = (",
                f'    spark.read.table("{src_tb.catalog}.{src_tb.schema}.{src_tb.name}")',
            ]

            if getattr(src_tb, "filter", ""):
                block_lines.append(f"    .filter({src_tb.filter})")

            block_lines.append(")")

            if spec.final_table.flg_is_HR_table:
                block_lines.append(f"df_{src_tb.name} = decrypt_any(df_{src_tb.name})")

            block_lines.append("")
            all_blocks.append("\n".join(block_lines))

        return [
            md_cell("## Read From Source Tables"),
            py_cell("\n\n".join(all_blocks)),
        ]