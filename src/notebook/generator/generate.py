import src.notebook.sections as s
from src.notebook.renderers import render_notebook
from src.notebook.config.paths import NotebookPaths
from src.notebook.generator.generator import NotebookGenerator


def get_sections_knowing_notebook_type(kind, paths):
    if kind == "table":
        return [
        s.HeaderSection(),
        s.UtilitiesSection(paths),
        s.FunctionDeclarationSection(),
        s.WidgetsSection(),
        s.PathsSection(),
        s.StartControlSection(),
        s.TableCreationSection(),
        s.TableTagsSection(),
        s.DependenciesSection(),
        s.ReadSourcesSection(),
        s.BusinessLogicSection(),
        s.ExitIfEmptySection(),
        s.EncryptionSection(),
        s.StagingSection(),
        s.MergeDWSection(),
        s.ViewCreationSection(),
        s.EndControlSection(),
    ]

    if kind == "view":
        return [s.StandAloneViewCreationSection()]

    raise ValueError(f"Unknown kind: {kind}")


def generate_notebook_content(nb, kind="table"):
    paths = NotebookPaths()
    sections = get_sections_knowing_notebook_type(kind, paths)
    generator = NotebookGenerator(sections)
    return generator.generate(nb)
