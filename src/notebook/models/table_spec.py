## Table class (stores table metadata)
class TableSpec:
  def __init__(self, name, schema, catalog):
    self.name = name
    self.schema = schema
    self.catalog = catalog
    self.type_abv = self.load_table_type_abv()
    self.type = self.load_table_type()
    self.scd_type = None
  
  def load_table_type_abv(self):
    if self.name[:3] == 'agg':
      return 'agg'
    if self.name[:3] == 'fac':
      return 'fac'
    if self.name[:3] == 'dim':
      return 'dim'
    if self.name[:3] == 'cfg':
      return 'cfg'
    if self.name[:2] == 'vw':
      return 'vw'
    if self.name[:3] == 'rel':
      return 'rel'
    if self.name[:3] == 'wdl':
      return 'cfg'
    if self.schema == 'curated':
      return 'cur'
  
  def load_table_type(self):
    if self.name[:3] == 'agg':
      return 'aggregated'
    if self.name[:3] == 'fac':
      return 'factual'
    if self.name[:3] == 'dim':
      return 'dimension'
    if self.name[:3] == 'cfg':
      return 'configuration'
    if self.name[:3] == 'vw':
      return 'view'
    if self.name[:3] == 'rel':
      return 'relation'
    if self.name[:3] == 'wdl':
      return 'configuration'
    if self.schema == 'curated':
      return 'Curated'
  
  def load_scd_type(self):
    return
  
  def __str__(self):
    return f"\nTable Name: {self.name} \nSchema: {self.schema} \nCatalog: {self.catalog}"

### Complex and Simple Table Subclasses


## SourceTable class (stores source tables metadata)
class SimpleTable(TableSpec):
  def __init__(self, name, schema, catalog, condition):
    super().__init__(name, schema, catalog)
    self.filter = condition
  
  def __str__(self):
    return f"\nTable Name: {self.name} \nSchema: {self.schema} \nCatalog: {self.catalog} \nTable Type abv: {self.type_abv} \nTable Type: {self.type} \nFilter: {self.filter}"
  
## FinalTable class (stores final table metadata)
class ComplexTable(TableSpec):
  def __init__(self, name, schema, catalog, description, column_list, pk='N/A', scd_type='N/A', columns_to_update='N/A', condition=""):
    super().__init__(name, schema, catalog)
    
    self.description = description
  
    self.flg_is_HR_table = True if self.catalog == 'people_dev' else False
    self.view_name = 'vw_' + self.name
    self.view_schema = 'business_views' if self.flg_is_HR_table == False else 'dw'
    self.view_catalog = catalog
    self.view_description = description.replace('Table', 'View', 1)

    self.primary_key = pk
    self.scd_type = scd_type
    self.columns_to_update = columns_to_update
    self.columns = column_list

    self.filter = condition
  
  def __str__(self):
    columns_str = "\n".join([str(column) for column in self.columns])
    return f"\nTable Name: {self.name} \nSchema: {self.schema} \nCatalog: {self.catalog} \nTable Type abv: {self.type_abv} \nTable Type: {self.type} \nPrimary Key: {self.primary_key} \nColumns to update: {self.columns_to_update} \nTable Description: {self.description} \nView Description: {self.view_description} \nColumns: {columns_str}"
