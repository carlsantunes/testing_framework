import src.notebook.sections as s
from src.notebook.renderers import render_notebook
from src.notebook.config.paths import NotebookPaths
from src.notebook.generator.generator import NotebookGenerator

def generate_table_notebook_content(nb):
    paths = NotebookPaths()
    generator = NotebookGenerator([
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
        s.StagingSection(staging_schema=paths.staging_schema),
        s.MergeDWSection(staging_schema=paths.staging_schema),
        s.ViewCreationSection(),
        s.EndControlSection(staging_schema=paths.staging_schema),
    ])
    return generator.generate(nb)

def generate_view_notebook_content(nb):
    paths = NotebookPaths()
    generator = NotebookGenerator([s.StandAloneViewCreationSection()])
    return generator.generate(nb)
